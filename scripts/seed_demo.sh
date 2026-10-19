#!/bin/sh
set -e
PY=${PYTHON:-python}
SHOP=${SHOPIFY_MOCK_URL:-http://127.0.0.1:8081}
VF=${VIETFUL_MOCK_URL:-http://127.0.0.1:8082}
HOOK=${WEBHOOK_URL:-http://127.0.0.1:8090}

$PY -m cdms init-db
$PY -m cdms tenant add acme   --name "Acme Retail"
$PY -m cdms tenant add globex --name "Globex Mart"

for T in acme globex; do
  $PY -m cdms worker add "$T-vietful" --tenant "$T" --kind vietful --interval 0 \
    --config "{\"stream\": \"vietful:$T\", \"api_base\": \"$VF\", \"client_id\": \"$T\", \"client_secret\": \"$T-secret\"}"
  $PY -m cdms vietful subscribe "$T-vietful" --endpoint "$HOOK/webhooks/vietful/$T"
done

$PY -m cdms worker add acme-shopify   --tenant acme   --kind shopify --interval 15 \
  --config "{\"shop_url\": \"$SHOP/acme-shop\", \"access_token\": \"acme-shop-token\"}"
$PY -m cdms worker add globex-shopify --tenant globex --kind shopify --interval 60 \
  --config "{\"shop_url\": \"$SHOP/globex\", \"access_token\": \"globex-token\"}"

for W in acme-vietful globex-vietful acme-shopify globex-shopify; do $PY -m cdms worker start "$W"; done
