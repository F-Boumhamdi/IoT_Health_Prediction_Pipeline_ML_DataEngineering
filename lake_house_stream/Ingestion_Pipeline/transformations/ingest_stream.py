# =========================================================
# ingest_stream.py
# Azure Event Hubs → Databricks Bronze
# =========================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

# =========================================================
# EVENT HUBS CONFIG
# =========================================================

EH_NAMESPACE = "ehiotmlhealthns"
EH_NAME = "ehiotmlhealthtopic"
EH_CONN_STR = spark.conf.get("connection_string")

# =========================================================
# KAFKA API CONFIG FOR EVENT HUBS
# =========================================================

KAFKA_OPTIONS = {
    # Azure Event Hubs Kafka Endpoint
    "kafka.bootstrap.servers":
        f"{EH_NAMESPACE}.servicebus.windows.net:9093",
    # Event Hub name
    "subscribe":
        EH_NAME,
    # Security
    "kafka.security.protocol":
        "SASL_SSL",
    "kafka.sasl.mechanism":
        "PLAIN",
    "kafka.sasl.jaas.config":
        f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{EH_CONN_STR}";',
    # Streaming behavior
    "startingOffsets":
        "earliest",
    "maxOffsetsPerTrigger":
        "10000",
    "failOnDataLoss":
        "false",
    # Stability
    "kafka.request.timeout.ms":
        "60000",
    "kafka.session.timeout.ms":
        "30000"
}

# =========================================================
# JSON SCHEMA
# =========================================================

schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("caseid", IntegerType(), True),
    StructField("SNUADC_ART_SBP", DoubleType(), True),
    StructField("Solar8000_HR", DoubleType(), True),
    StructField("Solar8000_PLETH_SPO2", DoubleType(), True),
    StructField("Solar8000_BT", DoubleType(), True),
    StructField("Device_Battery_Level", DoubleType(), True),
    StructField("Operator_ID", IntegerType(), True)
])

# =========================================================
# BRONZE TABLE
# =========================================================

@dp.table(
    name="iot_stream_raw",
    comment="Raw IoT streaming ingestion from Azure Event Hubs"
)

def iot_stream_raw():

    # =====================================================
    # 1. READ STREAM
    # =====================================================

    kafka_df = (
        spark.readStream
        .format("kafka")
        .options(**KAFKA_OPTIONS)
        .load()
    )

    # =====================================================
    # 2. CONVERT KAFKA BYTES
    # =====================================================

    json_df = (
        kafka_df.selectExpr(
            "CAST(key AS STRING) as kafka_key",
            "CAST(value AS STRING) as json_value",
            "topic",
            "partition",
            "offset",
            "timestamp as kafka_timestamp"
        )
    )

    # =====================================================
    # 3. PARSE JSON
    # =====================================================

    parsed_df = (
        json_df.withColumn(
            "parsed_data",
            from_json(
                col("json_value"),
                schema
            )
        )
    )

    # =====================================================
    # 4. EXPAND JSON
    # =====================================================

    expanded_df = (
        parsed_df.select(
            "parsed_data.*",
            "kafka_key",
            "topic",
            "partition",
            "offset",
            "kafka_timestamp"
        )
    )

    # =====================================================
    # 5. FINAL BRONZE FORMAT
    # =====================================================

    bronze_df = (
        expanded_df
        # convert timestamp
        .withColumn(
            "timestamp",
            to_timestamp(col("timestamp"))
        )

        # ingestion metadata
        .withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )

        .withColumn(
            "source_type",
            lit("azure_eventhubs_stream")
        )

        # ingestion quality
        .withColumn(
            "is_valid_json",
            col("caseid").isNotNull()
        )
    )

    # =====================================================
    # 6. RETURN STREAM
    # =====================================================

    return bronze_df