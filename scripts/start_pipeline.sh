#!/bin/bash
#
# Start CDC Pipeline
# Runs the Spark streaming application
#

set -e

echo "=================================================================="
echo "🚀 STARTING CDC SPARK PIPELINE"
echo "=================================================================="
echo ""

# Load environment variables from .env file
if [ -f .env ]; then
    echo "📝 Loading environment variables from .env"
    export $(grep -v '^#' .env | xargs)
    echo "✅ Environment variables loaded"
else
    echo "⚠️  Warning: .env file not found, using defaults"
fi
echo ""

# Check if infrastructure is running
if ! docker ps | grep -q postgres; then
    echo "❌ Error: Infrastructure not running"
    echo "   Please run: ./scripts/setup_infrastructure.sh"
    exit 1
fi

echo "✅ Infrastructure is running"
echo ""

# Set default values from .env or use hardcoded defaults
CHECKPOINT_LOCATION=${CHECKPOINT_LOCATION:-/tmp/spark_checkpoints}
WAREHOUSE_LOCATION=${WAREHOUSE_LOCATION:-/tmp/cdc_warehouse}
DLQ_LOCATION=${DLQ_LOCATION:-/tmp/cdc_dead_letter}
KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}

# Create directories if they don't exist
mkdir -p "$WAREHOUSE_LOCATION"
mkdir -p "$CHECKPOINT_LOCATION"
mkdir -p "$DLQ_LOCATION"

echo "📁 Warehouse Location: $WAREHOUSE_LOCATION"
echo "📁 Checkpoints: $CHECKPOINT_LOCATION"
echo "📁 Dead Letter Queue: $DLQ_LOCATION"
echo "📡 Kafka Bootstrap: $KAFKA_BOOTSTRAP_SERVERS"
echo ""

echo "⏱️  Pipeline will process CDC events every 30 seconds"
echo "⏹️  Press Ctrl+C to stop gracefully"
echo ""

# Export environment variables for Spark
export KAFKA_BOOTSTRAP_SERVERS
export CHECKPOINT_LOCATION
export WAREHOUSE_LOCATION
export DLQ_LOCATION

# Set PySpark configuration
export PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --master local[*] pyspark-shell"

# Run Spark application with Python
/Users/samueldelilaj/miniconda3/bin/python src/pipeline/spark_cdc_app.py

echo ""
echo "=================================================================="
echo "✅ PIPELINE STOPPED"
echo "=================================================================="
echo ""
echo "📊 Check warehouse:"
echo "   ls -lh $WAREHOUSE_LOCATION""
echo ""
echo "📓 View analytics:"
echo "   jupyter notebook notebooks/analytics_demo.ipynb"
echo ""
