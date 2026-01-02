"""
Production CDC Spark Streaming Application
==========================================

Features:
- Configurable Kafka bootstrap servers (env var)
- Proper schema evolution with PERMISSIVE mode
- Corrupt record handling to DLQ
- Fixed partitioning and query management
- Proper timestamp conversions
- Query naming and progress tracking
- Soft delete support for all tables

Run: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_cdc_app.py
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CDCSparkApplication:
    
    def __init__(self):
        logger.info("🚀 Initializing Production CDC Spark Application")
        
        # Configuration from environment variables
        self.kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
        self.checkpoint_dir = os.getenv("CHECKPOINT_LOCATION", "/tmp/spark_checkpoints")
        self.warehouse_dir = os.getenv("WAREHOUSE_LOCATION", "/tmp/cdc_warehouse")
        self.dead_letter_dir = os.getenv("DLQ_LOCATION", "/tmp/cdc_dead_letter")
        
        logger.info(f"📡 Kafka: {self.kafka_servers}")
        logger.info(f"💾 Warehouse: {self.warehouse_dir}")
        logger.info(f"⚠️  DLQ: {self.dead_letter_dir}")
        
        self.spark = SparkSession.builder \
            .appName("ProductionCDCPipeline") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        self.active_queries = []
        
        logger.info("✅ Spark session created")
    
    # ============================================
    # SCHEMAS WITH SCHEMA EVOLUTION
    # ============================================
    
    def get_user_schema(self):
        """Schema for users table with corrupt record tracking"""
        return StructType([
            StructField("user_id", IntegerType(), True),
            StructField("username", StringType(), True),
            StructField("email", StringType(), True),
            StructField("full_name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("updated_at", StringType(), True),
            StructField("_corrupt_record", StringType(), True)
        ])
    
    def get_order_schema(self):
        """Schema for orders table with corrupt record tracking"""
        return StructType([
            StructField("order_id", IntegerType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("order_number", StringType(), True),
            StructField("status", StringType(), True),
            StructField("total_amount", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("updated_at", StringType(), True),
            StructField("_corrupt_record", StringType(), True)
        ])
    
    def get_device_schema(self):
        """Schema for devices table with corrupt record tracking"""
        return StructType([
            StructField("device_id", IntegerType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("device_type", StringType(), True),
            StructField("device_name", StringType(), True),
            StructField("os_version", StringType(), True),
            StructField("app_version", StringType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("registered_at", StringType(), True),
            StructField("last_active", StringType(), True),
            StructField("_corrupt_record", StringType(), True)
        ])
    
    def get_cdc_envelope_schema(self, payload_schema):
        """Debezium CDC envelope with corrupt record tracking"""
        return StructType([
            StructField("before", payload_schema, True),
            StructField("after", payload_schema, True),
            StructField("source", StructType([
                StructField("table", StringType(), True),
                StructField("db", StringType(), True),
                StructField("ts_ms", LongType(), True)
            ]), True),
            StructField("op", StringType(), True),
            StructField("ts_ms", LongType(), True),
            StructField("_corrupt_record", StringType(), True)
        ])
    
    # ============================================
    # DATA INGESTION FROM KAFKA
    # ============================================
    
    def read_from_kafka(self, topic):
        """Read streaming data from Kafka"""
        logger.info(f"📥 Reading from Kafka topic: {topic}")
        
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", "1000") \
            .load()
        
        logger.info(f"✅ Connected to topic: {topic}")
        return df
    
    # ============================================
    # PARSING WITH SCHEMA EVOLUTION
    # ============================================
    
    def parse_with_schema_evolution(self, df, schema, table_name):
        """
        Parse JSON with PERMISSIVE mode for schema evolution
        Routes corrupt records to DLQ automatically
        """
        logger.info(f"🔍 Parsing {table_name} with schema evolution support")
        
        # Parse JSON with PERMISSIVE mode
        parsed_df = df.select(
            col("key").cast("string").alias("event_key"),
            col("value").cast("string").alias("raw_value"),
            col("topic"),
            col("timestamp").alias("kafka_timestamp"),
            from_json(
                col("value").cast("string"),
                schema,
                {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"}
            ).alias("parsed")
        )
        
        # Split valid and corrupt records
        valid_df = parsed_df.filter(
            col("parsed._corrupt_record").isNull() & col("parsed").isNotNull()
        )
        
        corrupt_df = parsed_df.filter(
            col("parsed._corrupt_record").isNotNull() | col("parsed").isNull()
        )
        
        # Write corrupt records to DLQ
        dlq_query = self.write_to_dlq(corrupt_df, table_name)
        if dlq_query:
            self.active_queries.append(dlq_query)
        
        logger.info(f"✅ Parsing complete for {table_name}")
        return valid_df
    
    def write_to_dlq(self, df, table_name):
        """Write corrupt/malformed records to Dead Letter Queue"""
        query_name = f"dlq_{table_name}"
        
        query = df.select(
            col("raw_value"),
            col("topic"),
            col("kafka_timestamp"),
            col("parsed._corrupt_record").alias("corrupt_record"),
            current_timestamp().alias("dlq_timestamp"),
            lit("CORRUPT_RECORD").alias("error_type"),
            lit(table_name).alias("source_table")
        ) \
            .writeStream \
            .queryName(query_name) \
            .outputMode("append") \
            .format("json") \
            .option("path", f"{self.dead_letter_dir}/{table_name}") \
            .option("checkpointLocation", f"{self.checkpoint_dir}/dlq_{table_name}") \
            .trigger(processingTime="30 seconds") \
            .start()
        
        logger.info(f"⚠️  DLQ stream started: {query_name} (id: {query.id})")
        return query
    
    # ============================================
    # DATA TRANSFORMATION
    # ============================================
    
    def transform_users(self, df):
        """Transform user data with proper timestamp conversion and soft deletes"""
        logger.info("🔄 Transforming users")
        
        # Extract and convert timestamps
        transformed = df.select(
            coalesce(col("parsed.after.user_id"), col("parsed.before.user_id")).alias("user_id"),
            coalesce(col("parsed.after.username"), col("parsed.before.username")).alias("username_raw"),
            coalesce(col("parsed.after.email"), col("parsed.before.email")).alias("email_raw"),
            coalesce(col("parsed.after.full_name"), col("parsed.before.full_name")).alias("full_name_raw"),
            coalesce(col("parsed.after.status"), col("parsed.before.status")).alias("status_raw"),
            to_timestamp(coalesce(col("parsed.after.created_at"), col("parsed.before.created_at"))).alias("created_at"),
            to_timestamp(coalesce(col("parsed.after.updated_at"), col("parsed.before.updated_at"))).alias("updated_at"),
            (col("parsed.ts_ms") / 1000).cast(TimestampType()).alias("event_time"),
            col("parsed.op").alias("cdc_operation")
        ).filter(col("user_id").isNotNull())
        
        # Clean and enrich
        enriched = transformed \
            .withColumn("username", trim(lower(col("username_raw")))) \
            .withColumn("email", trim(lower(col("email_raw")))) \
            .withColumn("full_name", trim(col("full_name_raw"))) \
            .withColumn("status", upper(col("status_raw"))) \
            .withColumn("email_domain", regexp_extract(col("email"), "@(.+)$", 1)) \
            .withColumn("username_length", length(col("username"))) \
            .withColumn("has_full_name", col("full_name").isNotNull()) \
            .withColumn("is_valid_email", col("email").rlike("^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,}$")) \
            .withColumn("is_valid_username", (col("username_length") >= 3) & (col("username_length") <= 50)) \
            .withColumn("data_quality_score", 
                       (col("is_valid_email").cast("int") + col("is_valid_username").cast("int") + 
                        col("has_full_name").cast("int")) / 3.0 * 100) \
            .withColumn("is_deleted", when(col("cdc_operation") == "d", lit(True)).otherwise(lit(False))) \
            .withColumn("processed_at", current_timestamp()) \
            .withColumn("processing_date", current_date())
        
        return enriched.select(
            "user_id", "username", "email", "email_domain", "full_name", "status",
            "username_length", "is_valid_email", "is_valid_username", "data_quality_score",
            "has_full_name", "created_at", "updated_at", "event_time", "cdc_operation",
            "is_deleted", "processed_at", "processing_date"
        )
    
    def transform_orders(self, df):
        """Transform order data with proper timestamp conversion and soft deletes"""
        logger.info("🔄 Transforming orders")
        
        transformed = df.select(
            coalesce(col("parsed.after.order_id"), col("parsed.before.order_id")).alias("order_id"),
            coalesce(col("parsed.after.user_id"), col("parsed.before.user_id")).alias("user_id"),
            coalesce(col("parsed.after.order_number"), col("parsed.before.order_number")).alias("order_number"),
            coalesce(col("parsed.after.status"), col("parsed.before.status")).alias("status_raw"),
            coalesce(col("parsed.after.total_amount"), col("parsed.before.total_amount")).alias("total_amount_raw"),
            coalesce(col("parsed.after.currency"), col("parsed.before.currency")).alias("currency"),
            to_timestamp(coalesce(col("parsed.after.created_at"), col("parsed.before.created_at"))).alias("created_at"),
            to_timestamp(coalesce(col("parsed.after.updated_at"), col("parsed.before.updated_at"))).alias("updated_at"),
            (col("parsed.ts_ms") / 1000).cast(TimestampType()).alias("event_time"),
            col("parsed.op").alias("cdc_operation")
        ).filter(col("order_id").isNotNull())
        
        enriched = transformed \
            .withColumn("status", upper(col("status_raw"))) \
            .withColumn("total_amount", col("total_amount_raw").cast(DoubleType())) \
            .withColumn("order_year", year(col("created_at"))) \
            .withColumn("order_month", month(col("created_at"))) \
            .withColumn("order_day", dayofmonth(col("created_at"))) \
            .withColumn("amount_category",
                       when(col("total_amount") < 100, lit("LOW"))
                       .when(col("total_amount") < 500, lit("MEDIUM"))
                       .when(col("total_amount") < 1000, lit("HIGH"))
                       .otherwise(lit("PREMIUM"))) \
            .withColumn("is_high_value", col("total_amount") > 1000) \
            .withColumn("is_deleted", when(col("cdc_operation") == "d", lit(True)).otherwise(lit(False))) \
            .withColumn("processed_at", current_timestamp()) \
            .withColumn("processing_date", current_date())
        
        return enriched.select(
            "order_id", "user_id", "order_number", "status", "total_amount", "currency",
            "amount_category", "is_high_value", "order_year", "order_month", "order_day",
            "created_at", "updated_at", "event_time", "cdc_operation", "is_deleted",
            "processed_at", "processing_date"
        )
    
    def transform_devices(self, df):
        """Transform device data with proper timestamp conversion and soft deletes"""
        logger.info("🔄 Transforming devices")
        
        transformed = df.select(
            coalesce(col("parsed.after.device_id"), col("parsed.before.device_id")).alias("device_id"),
            coalesce(col("parsed.after.user_id"), col("parsed.before.user_id")).alias("user_id"),
            coalesce(col("parsed.after.device_type"), col("parsed.before.device_type")).alias("device_type_raw"),
            coalesce(col("parsed.after.device_name"), col("parsed.before.device_name")).alias("device_name"),
            coalesce(col("parsed.after.os_version"), col("parsed.before.os_version")).alias("os_version"),
            coalesce(col("parsed.after.app_version"), col("parsed.before.app_version")).alias("app_version"),
            coalesce(col("parsed.after.is_active"), col("parsed.before.is_active")).alias("is_active"),
            to_timestamp(coalesce(col("parsed.after.registered_at"), col("parsed.before.registered_at"))).alias("registered_at"),
            to_timestamp(coalesce(col("parsed.after.last_active"), col("parsed.before.last_active"))).alias("last_active"),
            (col("parsed.ts_ms") / 1000).cast(TimestampType()).alias("event_time"),
            col("parsed.op").alias("cdc_operation")
        ).filter(col("device_id").isNotNull())
        
        enriched = transformed \
            .withColumn("device_type", upper(col("device_type_raw"))) \
            .withColumn("platform",
                       when(col("device_type").contains("IOS"), lit("iOS"))
                       .when(col("device_type").contains("ANDROID"), lit("Android"))
                       .when(col("device_type").contains("WEB"), lit("Web"))
                       .otherwise(lit("Other"))) \
            .withColumn("is_mobile", col("platform").isin(["iOS", "Android"])) \
            .withColumn("is_deleted", when(col("cdc_operation") == "d", lit(True)).otherwise(lit(False))) \
            .withColumn("processed_at", current_timestamp()) \
            .withColumn("processing_date", current_date())
        
        return enriched.select(
            "device_id", "user_id", "device_type", "device_name", "os_version", "app_version",
            "platform", "is_mobile", "is_active", "registered_at", "last_active", "event_time",
            "cdc_operation", "is_deleted", "processed_at", "processing_date"
        )
    
    # ============================================
    # WAREHOUSE SINK
    # ============================================
    
    def write_to_warehouse(self, df, table_name):
        """Write to warehouse with proper partitioning and query management"""
        logger.info(f"💾 Writing {table_name} to warehouse")
        
        query_name = f"warehouse_{table_name}"
        warehouse_path = f"{self.warehouse_dir}/{table_name}"
        checkpoint_path = f"{self.checkpoint_dir}/{table_name}"
        
        # Determine partition columns
        if "processing_date" in df.columns:
            partition_cols = ["processing_date"]
        elif "order_year" in df.columns:
            partition_cols = ["order_year", "order_month"]
        else:
            partition_cols = []
        
        # Build write stream with proper chaining
        writer = df.writeStream \
            .queryName(query_name) \
            .outputMode("append") \
            .format("parquet") \
            .option("path", warehouse_path) \
            .option("checkpointLocation", checkpoint_path) \
            .trigger(processingTime="30 seconds")
        
        # Add partitioning if applicable
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        # Start the query
        query = writer.start()
        
        logger.info(f"✅ Warehouse stream started: {query_name} (id: {query.id})")
        return query
    
    # ============================================
    # PIPELINE ORCHESTRATION
    # ============================================
    
    def process_table(self, topic, table_name):
        """Complete processing pipeline for one table"""
        logger.info(f"\n{'='*60}")
        logger.info(f"📊 Processing: {table_name}")
        logger.info(f"{'='*60}")
        
        # 1. Read from Kafka
        raw_df = self.read_from_kafka(topic)
        
        # 2. Parse with schema evolution
        if table_name == "users":
            schema = self.get_cdc_envelope_schema(self.get_user_schema())
        elif table_name == "orders":
            schema = self.get_cdc_envelope_schema(self.get_order_schema())
        elif table_name == "devices":
            schema = self.get_cdc_envelope_schema(self.get_device_schema())
        else:
            raise ValueError(f"Unknown table: {table_name}")
        
        parsed_df = self.parse_with_schema_evolution(raw_df, schema, table_name)
        
        # 3. Transform
        if table_name == "users":
            transformed_df = self.transform_users(parsed_df)
        elif table_name == "orders":
            transformed_df = self.transform_orders(parsed_df)
        elif table_name == "devices":
            transformed_df = self.transform_devices(parsed_df)
        else:
            raise ValueError(f"Unknown table: {table_name}")
        
        # 4. Write to warehouse
        warehouse_query = self.write_to_warehouse(transformed_df, table_name)
        
        logger.info(f"✅ Pipeline ready for {table_name}")
        return warehouse_query
    
    def run(self):
        """Run the complete CDC pipeline"""
        logger.info("\n" + "="*60)
        logger.info("🚀 PRODUCTION CDC PIPELINE")
        logger.info("="*60)
        logger.info(f"📡 Kafka: {self.kafka_servers}")
        logger.info(f"💾 Warehouse: {self.warehouse_dir}")
        logger.info(f"⚠️  DLQ: {self.dead_letter_dir}")
        logger.info("="*60 + "\n")
        
        try:
            # Process all tables
            tables = [
                ("cdc.public.users", "users"),
                ("cdc.public.orders", "orders"),
                ("cdc.public.devices", "devices")
            ]
            
            for topic, table_name in tables:
                query = self.process_table(topic, table_name)
                self.active_queries.append(query)
            
            logger.info(f"\n✅ All {len(self.active_queries)} streams started!")
            logger.info("📊 Active queries:")
            for q in self.active_queries:
                logger.info(f"   - {q.name}: {q.id}")
            
            logger.info("\n⏱️  Processing every 30 seconds")
            logger.info("Press Ctrl+C to stop\n")
            
            # Wait for any query to terminate
            self.spark.streams.awaitAnyTermination()
        
        except KeyboardInterrupt:
            logger.info("\n⚠️  Shutting down gracefully...")
            for q in self.active_queries:
                q.stop()
            self.spark.stop()
            logger.info("✅ Application stopped")
        
        except Exception as e:
            logger.error(f"\n❌ Fatal error: {str(e)}")
            import traceback
            traceback.print_exc()
            for q in self.active_queries:
                q.stop()
            self.spark.stop()
            raise


if __name__ == "__main__":
    app = CDCSparkApplication()
    app.run()
