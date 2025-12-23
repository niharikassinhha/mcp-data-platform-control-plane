DATA_FLOWS = {
    "order_created": {
        "source": {
            "type": "event",
            "producer": "order-service",
            "transport": "Amazon Kinesis Data Streams"
        },
        "ingestion": {
            "job": "glue-streaming-order-created",
            "mode": "streaming",
            "schema_enforced": True
        },
        "layers": [
            {
                "layer": "bronze",
                "table": "bronze_order_created",
                "characteristics": [
                    "append-only",
                    "immutable",
                    "partitioned by ingest_time"
                ]
            },
            {
                "layer": "silver",
                "table": "silver_order_created",
                "characteristics": [
                    "deduplicated",
                    "event-time partitioned",
                    "upsert via Iceberg MERGE"
                ]
            },
            {
                "layer": "gold",
                "table": "gold_daily_order_metrics",
                "characteristics": [
                    "aggregated",
                    "analytics-optimized",
                    "rebuildable"
                ]
            }
        ],
        "slas": {
            "bronze_freshness_minutes": 5,
            "silver_latency_minutes": 30,
            "gold_refresh": "daily"
        },
        "replay_strategy": {
            "source_of_truth": "bronze",
            "replay_path": "bronze → silver → gold",
            "max_late_data_hours": 24
        }
    }
}