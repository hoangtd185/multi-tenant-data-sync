# Hướng dẫn CDMS: code chạy thế nào và cách chạy

CDMS (Change Data Management System) là service **đồng bộ dữ liệu sản phẩm + tồn kho cho nhiều tenant**.
Dữ liệu được lấy từ 2 nguồn bên ngoài, chuẩn hoá và kiểm tra, rồi ghi vào PostgreSQL:

- **Vietful** (kho WMS): kiểu *push*. Vietful bắn webhook khi tồn kho đổi → CDMS đẩy vào Redis Stream → worker đọc ra rồi gọi API Vietful lấy dữ liệu mới nhất.
- **Shopify** (shop online): kiểu *pull*. Worker gọi GraphQL Shopify theo lịch để lấy những sản phẩm mới thay đổi.

Mỗi tenant có **schema riêng** (`t_<tenant>`), cộng thêm một bảng **master** dùng chung (`master.products`, khoá theo GTIN).

---

## 1. Bức tranh tổng thể

```mermaid
flowchart LR
    subgraph Nguồn dữ liệu
        VF[Vietful API<br/>mock :8082]
        SH[Shopify GraphQL<br/>mock :8081]
    end

    VF -- "webhook INV_CHANGED<br/>(ký HMAC)" --> WH[webhook-server :8090<br/>cdms/webhook.py]
    WH -- XADD --> RS[(Redis Stream<br/>vietful:&lt;tenant&gt;)]

    subgraph Agent - mỗi node 1 process
        AG[run_agent<br/>cdms/worker.py] --> W1[Thread: worker vietful]
        AG --> W2[Thread: worker shopify]
    end

    RS -- XREAD --> W1
    W1 -- "REST: Products,<br/>inventories" --> VF
    W2 -- "GraphQL products<br/>(updated_at >= since)" --> SH

    W1 & W2 --> PB[process_batch<br/>parse → normalize → store]
    PB --> PG[(PostgreSQL<br/>cdms.* / t_tenant.* / master.*)]
```

Có 3 loại process:

| Process | Lệnh | Vai trò |
|---|---|---|
| **webhook-server** | `python -m cdms webhook-server` | Nhận webhook Vietful, kiểm tra chữ ký, đẩy vào Redis Stream |
| **agent** | `python -m cdms agent` | Chạy trên mỗi node, tự nhận các worker cần chạy và mỗi worker chạy trong 1 thread |
| **cli** | `python -m cdms <lệnh>` | Quản trị: tạo tenant/worker, start/stop, xem thống kê |

Hai mock (`mock shopify-server`, `mock vietful-api`) đóng vai Shopify/Vietful thật để demo và test. Chúng cố tình
trả lỗi 429/500 và dữ liệu hỏng theo một tỉ lệ nhất định.

---

## 2. Bản đồ file

```
cdms/
  __main__.py          entry: python -m cdms → cli.main()
  cli.py               parse lệnh (argparse) rồi gọi module tương ứng
  config.py            đọc biến môi trường (DSN, Redis, lease, batch size, NODE_ID)
  db.py                DDL: schema cdms/master + schema riêng cho từng tenant; connect()
  worker.py            ★ trái tim: agent, vòng lặp worker, lease, checkpoint, process_batch
  connectors/
    __init__.py        Batch (records, checkpoint, more) + build_connector(kind, config)
    vietful.py         VietfulClient (OAuth, throttle, retry) + VietfulConnector (stream/backfill/reconcile)
    shopify.py         ShopifyConnector (GraphQL, phân trang cursor, lọc theo updated_at)
  normalize.py         Item / Rejected, validate (GTIN, giá, số lượng, timestamp), map từng nguồn → Item
  store.py             upsert vào t_<tenant>.products + master.products, ghi rejected_records
  webhook.py           HTTP server nhận webhook, verify HMAC-SHA256, XADD vào Redis
  stats.py             query thống kê: worker đang active, danh sách worker, throughput/latency
  mocks/               mock Shopify, mock Vietful, sinh dữ liệu giả (có lỗi cố ý)
tests/
  test_normalize.py    unit test: GTIN, mapping, hash, HMAC (không cần DB)
  test_integration.py  integration test: cần PostgreSQL + Redis thật
scripts/seed_demo.sh   tạo 2 tenant (acme, globex), mỗi tenant 1 worker vietful + 1 worker shopify
docker-compose.yml     dựng cả hệ thống: postgres, redis, 2 mock, webhook, seed, 2 agent
```

---

## 3. Database (`db.py`)

**Schema điều khiển `cdms`:**

| Bảng | Ý nghĩa |
|---|---|
| `cdms.tenants` | Danh sách tenant |
| `cdms.workers` | Cấu hình + trạng thái từng worker: `kind`, `config` (JSON), `interval_seconds`, **`desired_state`** (running/stopped, do người dùng đặt), **`status`** (thực tế), **`checkpoint`** (đã sync tới đâu), **`owner` + `lease_until`** (node nào đang giữ), `last_heartbeat`, `last_error` |
| `cdms.sync_runs` | Mỗi batch xử lý xong ghi 1 dòng: số nhận/insert/update/không đổi/bị loại, latency |
| `cdms.rejected_records` | Bản ghi không hợp lệ và lý do |

**Schema theo tenant `t_<tenant>`:** bảng `products`, unique theo `(source, source_id)`. Có lưu `content_hash`
và `source_updated_at` để tránh ghi trùng hoặc ghi đè bằng dữ liệu cũ.

**Schema `master`:** `master.products` khoá theo **GTIN-14** (mã ngắn hơn được thêm số 0 bên trái), dùng chung
cho mọi tenant.

`tenant add` → `db.create_tenant()` → insert `cdms.tenants` + chạy `TENANT_DDL` để tạo schema `t_<id>`.
Tenant id phải khớp regex `^[a-z][a-z0-9_]{1,30}$` để tránh SQL injection qua tên schema.

---

## 4. Flow chính: từ lệnh `start` đến lúc dữ liệu vào DB

### 4.1 Điều khiển worker

```
cdms worker add ...   → INSERT/UPSERT cdms.workers (desired_state = 'stopped')
cdms worker start X   → UPDATE desired_state = 'running'
cdms worker stop X    → UPDATE desired_state = 'stopped'
```

CLI **không chạy worker trực tiếp**, chỉ đổi `desired_state` trong DB. Agent đọc DB rồi tự phản ứng theo.
Cách này giúp điều khiển được nhiều node mà không cần message bus.

### 4.2 Agent (`worker.run_agent`)

Cứ mỗi 2 giây:
1. Đọc toàn bộ `cdms.workers`.
2. Nếu worker có `desired_state = running`, node này chưa chạy nó, **và** nó đang rảnh (`owner` rỗng, là chính node này,
   hoặc lease đã hết hạn) → tạo thread `run_worker(...)`.
3. Nếu `desired_state = stopped` mà thread đang chạy → set event `stop`.
4. Khi nhận SIGINT/SIGTERM → báo stop cho mọi thread và chờ chúng kết thúc.

### 4.3 Vòng lặp worker (`worker.run_worker`)

```mermaid
flowchart TD
    A[claim: UPDATE workers SET owner=node, lease_until=now+LEASE_S<br/>WHERE desired_state='running' AND lease rảnh/hết hạn] -->|None| Z[Thoát: đã bị stop<br/>hoặc node khác đang giữ]
    A -->|OK| B[build_connector kind, config<br/>đọc checkpoint]
    B --> C{stop?}
    C -->|có| R[release: owner=NULL, status=stopped]
    C -->|không| D[claim lại = gia hạn lease + heartbeat]
    D -->|mất lease / bị stop| R
    D --> E[batch = connector.fetch checkpoint, BATCH_SIZE]
    E --> F[process_batch: parse → ghi DB + checkpoint<br/>trong 1 transaction]
    F -->|lỗi| G[rollback, ghi last_error,<br/>backoff 1s → 2s → ... tối đa 60s] --> C
    F --> H{batch.more?}
    H -->|còn| C
    H -->|hết| I[_idle: chờ interval_seconds,<br/>vừa chờ vừa gia hạn lease] --> C
```

Các điểm chính:
- **Lease** (`claim`): một worker chỉ chạy trên đúng **1 node** tại một thời điểm. Nếu node chết, lease hết hạn
  sau `CDMS_LEASE_S` giây và node khác nhận lại worker đó.
- **Checkpoint**: được lưu **cùng transaction** với dữ liệu. Khi stop/crash rồi chạy lại, worker tiếp tục đúng chỗ cũ,
  không mất và không trùng dữ liệu.
- **Heartbeat**: mỗi lần `claim` sẽ cập nhật `last_heartbeat`. Lệnh `worker list` hiện `dead` nếu heartbeat cũ hơn
  `CDMS_HEARTBEAT_TIMEOUT_S`.

### 4.4 `process_batch` (worker.py) → `normalize.py` → `store.py`

1. Với mỗi record thô trong batch, gọi `connector.parse(raw)` để ra `Item` (hợp lệ) hoặc `Rejected` (lỗi + payload).
2. Mở **1 transaction**:
   - `store.write_items()`:
     - `dedupe_batch`: trong cùng batch, mỗi `(source, source_id)` chỉ giữ bản mới nhất.
     - Upsert vào `t_<tenant>.products`. Chỉ UPDATE khi **nội dung đổi** (`content_hash` khác) **và** dữ liệu không cũ hơn
       (`source_updated_at` mới hơn hoặc bằng). Nhờ vậy ghi lặp lại nhiều lần vẫn cho cùng kết quả (idempotent), và event
       đến trễ không ghi đè được dữ liệu mới.
     - Nếu có barcode → upsert `master.products` theo GTIN-14. Chỉ **điền vào các trường đang NULL** (`COALESCE`),
       không ghi đè dữ liệu đã có.
   - `store.write_rejected()` → `cdms.rejected_records`.
   - Cập nhật `checkpoint`, xoá `last_error`.
   - Ghi 1 dòng `cdms.sync_runs`: các con số đếm + latency (thời điểm ghi trừ thời điểm thay đổi ở nguồn).

**Validate trong `normalize.py`:**
- GTIN 8/12/13/14 số phải đúng check digit. SKU dạng chữ thì không bị coi là barcode và cũng không báo lỗi.
- Giá phải là số và không âm. Số lượng phải là số nguyên và không âm. Timestamp phải đúng ISO-8601.
- Các trường bắt buộc: Vietful cần `productId`, `partnerSKU`, `productName`; Shopify cần `title`, `updatedAt`, ít nhất 1 variant
  có `id` và `sku`.
- Vietful: số lượng = tổng `availableQty` của các dòng tồn kho có `conditionTypeCode = NEW`.
- Shopify: **mỗi variant thành 1 Item**. Tên = `title - variantTitle`, trừ khi variant tên là "Default Title".

---

## 5. Flow Vietful chi tiết (push)

### 5.1 Webhook → Redis (`webhook.py`)

```
Vietful ── POST /webhooks/vietful/<tenant>  (header x-vf-hmacsha256 = base64(HMAC-SHA256(body, client_secret)))
        ──► webhook-server
             1. Tìm các worker vietful của <tenant> (cache 30s) → danh sách (client_secret, stream)
             2. So chữ ký với từng secret (compare_digest)
             3. Khớp     → XADD <stream> {payload: body} → 200
                Không khớp → ghi cdms.rejected_records → 401
```

Webhook-server **không xử lý nghiệp vụ**. Nó chỉ xác thực rồi bỏ vào hàng đợi, nên trả lời rất nhanh. Khi worker
đang stop, event vẫn nằm chờ trong Redis Stream, start lại là xử lý tiếp.

Để Vietful biết gửi webhook về đâu: `cdms vietful subscribe <worker> --endpoint <url>` (gọi `POST /api/v1/WebhookSubscribers`).

### 5.2 `VietfulConnector.fetch(checkpoint)` có 3 chế độ

| Chế độ | Khi nào | Làm gì |
|---|---|---|
| **Backfill** | Lần đầu (`backfill_done` chưa có) | Ghi nhớ vị trí cuối stream hiện tại, rồi phân trang `GET /api/v1/Products` lấy **toàn bộ** catalogue. Event đến trong lúc backfill không bị mất vì sau đó sẽ đọc từ vị trí đã ghi nhớ |
| **Reconcile** | Mỗi `reconcile_every_s` (mặc định 3600s) | Chạy backfill lại một lượt để vá các webhook bị mất |
| **Stream** | Bình thường | `XREAD` từ `last_id` (block 2s) → lọc event `INV_CHANGED` → gom các `partnerSKU` → gọi API lấy **trạng thái hiện tại** của các SKU đó |

Mỗi sản phẩm được dựng thành 1 "snapshot" gồm: `GET /Products` + `GET /Products/inventories` (gộp 50 SKU mỗi lần)
+ `GET /Products/{sku}` để lấy `hsCode` và `countryOfOrigin` (có cache).
Event JSON hỏng, event thiếu `partnerSKU`, hoặc SKU mà Vietful không biết → đưa vào Rejected.

**`VietfulClient`**: lấy token OAuth2 `client_credentials` (tự làm mới trước khi hết hạn và khi bị 401), giới hạn tốc độ
`rate_per_s` (mặc định 15 req/s), retry khi gặp 429/5xx/lỗi mạng với exponential backoff + jitter, có tôn trọng
`Retry-After`.

---

## 6. Flow Shopify chi tiết (pull)

`ShopifyConnector.fetch(checkpoint)`:
- Gọi GraphQL `products(first, after, query: "updated_at:>='<since>'", sortKey: UPDATED_AT)`.
- Checkpoint = `{since, cursor, max_seen}`:
  - Nếu còn trang tiếp → giữ nguyên `since`, lưu `cursor` (stop giữa chừng vẫn chạy tiếp đúng trang đó), `more = True`.
  - Nếu hết trang → `since = max_seen` (updatedAt lớn nhất đã thấy), xoá cursor, `more = False`. Worker nghỉ
    `interval_seconds` rồi poll lại.
- Dùng `>=` nên các bản ghi ở đúng mốc `since` sẽ được đọc lại. Không sao, vì `content_hash` giúp chúng không bị ghi lần nữa.
- Retry khi gặp 429/5xx/lỗi mạng, giống bên Vietful.

---

## 7. Cách chạy

### Cách 1: Docker (khuyên dùng, nhất là trên Windows)

```bash
docker compose up --build -d
```
Lệnh này dựng: `postgres`, `redis`, `shopify-mock`, `vietful-mock`. Sau đó service `seed` chạy `scripts/seed_demo.sh`
(init DB, tạo tenant `acme` và `globex`, 4 worker, subscribe webhook, start worker). Rồi tới `webhook` và **2 replica `agent`**
(giả lập 2 node).

Thử nghiệm:
```bash
docker compose run --rm cli active                 # bao nhiêu worker đang active
docker compose run --rm cli worker list            # trạng thái, node đang giữ, checkpoint

# Giả lập kho Vietful thay đổi tồn kho → webhook có ký được bắn về CDMS
docker compose run --rm cli mock vietful-change --client-id acme --count 50
# Giả lập Shopify đổi 20 sản phẩm
curl -X POST "http://localhost:8081/acme-shop/_mock/mutate?n=20"

docker compose run --rm cli stats                  # throughput / latency / tỉ lệ bị loại
docker compose run --rm cli rejected               # lý do bản ghi bị loại

# Thử stop / resume: webhook vẫn được nhận và chờ trong Redis
docker compose run --rm cli worker stop acme-vietful
docker compose run --rm cli mock vietful-change --client-id acme --count 20
docker compose run --rm cli worker start acme-vietful   # chạy tiếp từ checkpoint

docker compose logs -f agent                       # xem log worker
docker compose down -v                             # tắt và xoá dữ liệu
```

### Cách 2: Chạy local không dùng Docker

Cần Python 3.10+, PostgreSQL 14+, Redis 6.2+.

```bash
pip install -r requirements.txt
```

Đặt biến môi trường:
- bash: `export CDMS_DSN=postgresql://postgres:postgres@localhost:5432/cdms` và `export CDMS_REDIS_URL=redis://localhost:6379/0`
- PowerShell: `$env:CDMS_DSN="postgresql://postgres:postgres@localhost:5432/cdms"` và `$env:CDMS_REDIS_URL="redis://localhost:6379/0"`

Mở mỗi lệnh dưới đây ở **một terminal riêng**, theo đúng thứ tự:
```bash
python -m cdms mock shopify-server --port 8081
python -m cdms mock vietful-api --port 8082
sh scripts/seed_demo.sh                 # trên Windows chạy bằng Git Bash
python -m cdms webhook-server --port 8090
python -m cdms agent                    # mở thêm terminal chạy agent lần nữa để giả lập 2 node
```
Sau đó:
```bash
python -m cdms mock vietful-change --client-id acme --count 50
python -m cdms stats
```

### Danh sách lệnh CLI

| Lệnh | Tác dụng |
|---|---|
| `init-db` | Tạo schema `cdms` + `master` |
| `tenant add <id> [--name]` / `tenant list` | Tạo tenant (kèm schema `t_<id>`) / liệt kê |
| `worker add <id> --tenant T --kind vietful\|shopify --interval S --config '<json>'` | Tạo hoặc sửa worker |
| `worker start\|stop <id>` | Đổi `desired_state` |
| `worker run <id>` | Chạy 1 worker ngay trong process hiện tại (để debug) |
| `worker reset <id>` | Xoá checkpoint (worker phải stop trước) để sync lại từ đầu |
| `worker list` | Liệt kê worker |
| `agent` | Chạy agent |
| `webhook-server [--port]` | Chạy server nhận webhook |
| `vietful subscribe <worker> --endpoint URL` | Đăng ký URL webhook với Vietful |
| `active` / `stats [--tenant] [--since-minutes]` / `rejected [--limit]` | Thống kê |
| `mock shopify-server` / `mock vietful-api` / `mock vietful-change` | Các mock |

**Config theo loại worker:**
- `vietful`: `stream`, `api_base`, `client_id`, `client_secret`, và tuỳ chọn `auth_url`, `realm` (mặc định `aad`), `rate_per_s` (15), `reconcile_every_s` (3600)
- `shopify`: `shop_url`, `access_token`

**Biến môi trường** (`config.py`):

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `CDMS_DSN` | `postgresql://postgres@127.0.0.1:5432/cdms` | Kết nối PostgreSQL |
| `CDMS_REDIS_URL` | `redis://127.0.0.1:6379/0` | Kết nối Redis |
| `CDMS_LEASE_S` | 30 | Thời gian lease của worker (giây) |
| `CDMS_HEARTBEAT_TIMEOUT_S` | 30 | Quá thời gian này mà không có heartbeat thì worker bị coi là `dead` |
| `CDMS_BATCH_SIZE` | 200 | Số bản ghi tối đa mỗi batch |
| `CDMS_NODE_ID` | `hostname-pid` | Tên node (giá trị ghi vào `owner`) |

---

## 8. Test

```bash
python -m unittest discover -s tests -v
# hoặc trong Docker:
docker compose run --rm --entrypoint python cli -m unittest discover -s tests -v
```

- `test_normalize.py` (không cần DB): check digit GTIN, mapping Vietful/Shopify, hash ổn định, HMAC đúng định nghĩa của Vietful, mỗi lỗi cố ý đều bị loại.
- `test_integration.py` (cần PostgreSQL + Redis, nếu thiếu sẽ tự skip):
  - backfill Vietful → nhận webhook → stop → resume
  - reconcile vá lại webhook bị mất
  - webhook-server từ chối chữ ký sai
  - pull Shopify idempotent + bảng master dùng chung giữa các tenant
  - cùng một sản phẩm có mã UPC và EAN chỉ thành 1 dòng master
  - lease không cho 2 node chạy trùng 1 worker
  - Shopify chạy tiếp được khi đang dừng giữa chừng phân trang

---

## 9. Đọc code theo thứ tự nào

1. `cli.py`: xem có những lệnh gì
2. `db.py`: hiểu các bảng
3. `worker.py`: `run_agent` → `run_worker` → `process_batch`
4. `connectors/__init__.py` → `shopify.py` (đơn giản hơn) → `vietful.py`
5. `normalize.py` → `store.py`
6. `webhook.py`, `stats.py`
7. `tests/test_integration.py`: xem các kịch bản end-to-end
