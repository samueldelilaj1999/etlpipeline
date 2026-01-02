#!/bin/bash
#
# Stop Pipeline and Infrastructure
# Gracefully stops all services
#

set -e

echo "=================================================================="
echo "🛑 STOPPING CDC PIPELINE"
echo "=================================================================="
echo ""

# Stop Spark if running
if pgrep -f "spark_cdc_app.py" > /dev/null; then
    echo "⏹️  Stopping Spark pipeline..."
    pkill -SIGINT -f "spark_cdc_app.py" || true
    sleep 5
    echo "✅ Spark stopped"
else
    echo "ℹ️  Spark pipeline not running"
fi

# Stop Docker services
echo ""
echo "🐳 Stopping Docker services..."
docker-compose down
echo "✅ All services stopped"

echo ""
echo "=================================================================="
echo "✅ ALL SERVICES STOPPED"
echo "=================================================================="
echo ""
echo "📊 Data preserved at:"
echo "   /tmp/cdc_warehouse/        - Warehouse data"
echo "   /tmp/spark_checkpoints/    - Processing state"
echo ""
echo "🧹 To clean data:"
echo "   rm -rf /tmp/cdc_warehouse /tmp/spark_checkpoints /tmp/cdc_dead_letter"
echo ""
echo "🔄 To restart:"
echo "   ./scripts/setup_infrastructure.sh"
echo "   ./scripts/start_pipeline.sh"
echo ""
