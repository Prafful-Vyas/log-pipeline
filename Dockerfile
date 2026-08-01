FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

COPY pyproject.toml ./
COPY common ./common
COPY services ./services
COPY scripts ./scripts

RUN pip install --no-cache-dir .

CMD ["python", "-m", "services.indexer"]
