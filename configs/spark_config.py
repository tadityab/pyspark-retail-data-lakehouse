"""Spark configuration builder with environment-aware settings."""

KEY_SHUFFLE_PARTITIONS = "spark.sql.shuffle.partitions"
KEY_APP_NAME = "spark.app.name"


COMMON_CONFIGS = {
    KEY_SHUFFLE_PARTITIONS: "200",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    "spark.sql.parquet.mergeSchema": "false",
    "spark.sql.session.timeZone": "UTC",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.autoBroadcastJoinThreshold": str(50 * 1024 * 1024),
}


SPARK_CONFIGS = {
    "local": {
        **COMMON_CONFIGS,
        "spark.master": "local[*]",
        KEY_APP_NAME: "RetailDataLakehouse-Local",
        KEY_SHUFFLE_PARTITIONS: "8",
        "spark.driver.memory": "4g",
    },
    "docker": {
        **COMMON_CONFIGS,
        "spark.master": "spark://spark-master:7077",
        KEY_APP_NAME: "RetailDataLakehouse-Docker",
        KEY_SHUFFLE_PARTITIONS: "50",
        "spark.driver.memory": "2g",
        "spark.executor.memory": "2g",
        "spark.executor.cores": "2",
    },
    "production": {
        **COMMON_CONFIGS,
        KEY_APP_NAME: "RetailDataLakehouse-Prod",
        "spark.sql.adaptive.skewJoin.enabled": "true",
    },
}


def get_spark_config(env: str = "local") -> dict:
    """Return Spark configuration dict for the given environment."""
    if env not in SPARK_CONFIGS:
        raise ValueError(f"Unknown environment: {env}. Use: {list(SPARK_CONFIGS.keys())}")
    return SPARK_CONFIGS[env]
