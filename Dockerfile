FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY cdms ./cdms
COPY tests ./tests
COPY scripts ./scripts
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "-m", "cdms"]
CMD ["agent"]
