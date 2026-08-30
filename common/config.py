from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Kafka / Redpanda
    kafka_brokers: str = "redpanda:9092"
    topic_raw: str = "logs.raw"
    topic_security: str = "logs.security"
    topic_dlq: str = "logs.dlq"
    topic_raw_partitions: int = 12
    topic_security_partitions: int = 3

    # Postgres
    pg_dsn: str = "postgres://logs:logs@postgres:5432/logs"

    # Indexer
    workers: int = 4
    write_strategy: str = "batched"  # "batched" | "naive" (M2 benchmark baseline)
    batch_size: int = 1000
    batch_max_age_ms: int = 250
    queue_max: int = 20_000
    pg_pool_max: int = 6

    # Producer
    producer_rate: float = 5000
    producer_services: int = 8
    producer_duration: int = 0  # 0 = run forever
    producer_scenarios: str = "all"  # "all" | "none" | comma list

    # Alerter
    alert_sink: str = "stdout"  # "stdout" | "webhook"
    alert_dedup_window_s: int = 300
    alert_webhook_url: str = ""

    # Misc
    log_level: str = "INFO"
    metrics_port: int = 9101
    indexer_metrics_port: int = 9102


settings = Settings()
