"""SparkSession builder — singleton, environment-aware."""

import os
from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError
from configs.spark_config import get_spark_config


_spark_session = None
DELTA_CONFIG_KEYS = {
    "spark.sql.extensions",
    "spark.sql.catalog.spark_catalog",
}


def get_spark_session(env: str = None, app_name: str = None) -> SparkSession:
    """Return a configured SparkSession (singleton).

    Args:
        env: Environment name (local, docker, production).
             Defaults to SPARK_ENV environment variable or 'local'.
        app_name: Override the default app name.
    """
    global _spark_session
    if _spark_session is not None:
        return _spark_session

    env = env or os.getenv("SPARK_ENV", "local")
    config = get_spark_config(env)

    def _build_session(active_config: dict) -> SparkSession:
        builder = SparkSession.builder
        for key, value in active_config.items():
            builder = builder.config(key, value)
        if app_name:
            builder = builder.config("spark.app.name", app_name)
        return builder.getOrCreate()

    # First attempt: requested configuration (includes Delta for full feature mode).
    try:
        _spark_session = _build_session(config)
    except (Py4JJavaError, Exception) as exc:
        message = str(exc)
        delta_related = (
            "DeltaCatalog" in message
            or "delta" in message.lower()
            or "org.apache.spark.sql.delta" in message
        )
        if not delta_related:
            raise

        # Fallback for local/corporate environments where Delta jars are unavailable.
        fallback_config = {
            key: value for key, value in config.items() if key not in DELTA_CONFIG_KEYS
        }
        _spark_session = _build_session(fallback_config)

    _spark_session.sparkContext.setLogLevel("WARN")
    return _spark_session


def stop_spark_session():
    """Stop the active SparkSession and reset the singleton."""
    global _spark_session
    if _spark_session is not None:
        _spark_session.stop()
        _spark_session = None
