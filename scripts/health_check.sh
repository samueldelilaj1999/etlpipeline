#!/bin/bash
#
# Health Check Script
# Verifies all pipeline components are healthy
#

set -e

echo "=================================================================="
echo "🏥 CDC PIPELINE - HEALTH CHECK"
echo "=================================================================="
echo ""

# Check Docker
echo "🐳 Checking Docker..."
if docker info > /dev/null 2>&1; then
    echo "   ✅ Docker is running"
else
    echo "   ❌ Docker is not running"
    exit 1
fi

# Check PostgreSQL
echo ""
echo "🗄️  Checking PostgreSQL..."
if docker ps | grep -q postgres; then
    if docker exec postgres pg_isready -U postgres > /dev/null 2>&1; then
        echo "   ✅ PostgreSQL is running and ready"
        
        # Check tables
        table_count=$(docker exec postgres psql -U postgres -d sourcedb -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public'" | xargs)
        echo "   ✅ Tables: $table_count"
    else
        echo "   ⚠️  PostgreSQL is running but not ready"
    fi
else
    echo "   ❌ PostgreSQL is not running"
fi

# Check Kafka
echo ""
echo "📨 Checking Kafka..."
if docker ps | grep -q kafka; then
    echo "   ✅ Kafka is running"
    
    # Check topics
    topic_count=$(docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null | wc -l | xargs)
    echo "   ✅ Topics: $topic_count"
    
    # Check CDC topics
    cdc_topics=$(docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null | grep "cdc.public" | wc -l | xargs)
    echo "   ✅ CDC Topics: $cdc_topics"
else
    echo "   ❌ Kafka is not running"
fi

# Check Debezium
echo ""
echo "🔗 Checking Debezium..."
if docker ps | grep -q debezium; then
    echo "   ✅ Debezium Connect is running"
    
    # Check connector
    if curl -s http://localhost:8083/connectors 2>/dev/null | grep -q "postgres-source-connector"; then
        echo "   ✅ CDC Connector is registered"
        
        # Check connector status
        status=$(curl -s http://localhost:8083/connectors/postgres-source-connector/status 2>/dev/null | grep -o '"state":"[^"]*"' | head -1 | cut -d'"' -f4)
        if [ "$status" = "RUNNING" ]; then
            echo "   ✅ Connector Status: RUNNING"
        else
            echo "   ⚠️  Connector Status: $status"
        fi
    else
        echo "   ⚠️  CDC Connector not registered"
    fi
else
    echo "   ❌ Debezium Connect is not running"
fi

# Check Spark
echo ""
echo "⚡ Checking Spark Pipeline..."
if pgrep -f "spark_cdc_app.py" > /dev/null; then
    echo "   ✅ Spark pipeline is running"
else
    echo "   ⚠️  Spark pipeline is not running"
fi

# Check Warehouse
echo ""
echo "📦 Checking Data Warehouse..."
if [ -d "/tmp/cdc_warehouse" ]; then
    echo "   ✅ Warehouse directory exists"
    
    # Count tables
    table_dirs=$(ls -d /tmp/cdc_warehouse/*/ 2>/dev/null | wc -l | xargs)
    echo "   ✅ Tables: $table_dirs"
    
    # Check each table
    for table_dir in /tmp/cdc_warehouse/*/; do
        table_name=$(basename "$table_dir")
        file_count=$(find "$table_dir" -name "*.parquet" 2>/dev/null | wc -l | xargs)
        
        if [ "$file_count" -gt 0 ]; then
            size=$(du -sh "$table_dir" 2>/dev/null | cut -f1)
            echo "   ✅ $table_name: $file_count files, $size"
        else
            echo "   ⚠️  $table_name: No parquet files"
        fi
    done
else
    echo "   ⚠️  Warehouse directory does not exist"
fi

# Check Checkpoints
echo ""
echo "💾 Checking Checkpoints..."
if [ -d "/tmp/spark_checkpoints" ]; then
    checkpoint_count=$(ls /tmp/spark_checkpoints 2>/dev/null | wc -l | xargs)
    echo "   ✅ Checkpoint tables: $checkpoint_count"
else
    echo "   ⚠️  Checkpoint directory does not exist"
fi

# Summary
echo ""
echo "=================================================================="
echo "📊 HEALTH CHECK SUMMARY"
echo "=================================================================="
echo ""

# Calculate health score
health_score=0
total_checks=5

docker info > /dev/null 2>&1 && ((health_score++))
docker ps | grep -q postgres && ((health_score++))
docker ps | grep -q kafka && ((health_score++))
docker ps | grep -q debezium && ((health_score++))
[ -d "/tmp/cdc_warehouse" ] && [ "$(ls -A /tmp/cdc_warehouse 2>/dev/null)" ] && ((health_score++))

health_percentage=$((health_score * 100 / total_checks))

if [ $health_percentage -ge 80 ]; then
    echo "✅ System Health: $health_percentage% (Healthy)"
elif [ $health_percentage -ge 50 ]; then
    echo "⚠️  System Health: $health_percentage% (Degraded)"
else
    echo "❌ System Health: $health_percentage% (Critical)"
fi

echo ""
echo "🔧 Troubleshooting:"
echo "   - Start infrastructure: ./scripts/setup_infrastructure.sh"
echo "   - Start pipeline: ./scripts/start_pipeline.sh"
echo "   - View logs: docker logs <container-name>"
echo ""
