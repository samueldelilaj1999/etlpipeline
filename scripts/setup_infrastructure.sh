#!/bin/bash
#
# Setup Infrastructure
# Starts all required services for the CDC pipeline
#

set -e

echo "=================================================================="
echo "🚀 CDC PIPELINE - INFRASTRUCTURE SETUP"
echo "=================================================================="
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running"
    echo "   Please start Docker Desktop and try again"
    exit 1
fi

echo "✅ Docker is running"
echo ""

# Start all services
echo "📦 Starting all infrastructure services..."
docker-compose up -d



# Register Debezium Connector
echo "🔗 Registering Debezium CDC Connector..."
if [ -f "docker/scripts/register-connector.sh" ]; then
    bash docker/scripts/register-connector.sh
fi

echo ""
echo "=================================================================="
echo "✅ INFRASTRUCTURE SETUP COMPLETE"
echo "=================================================================="
echo ""
echo "📊 Running Services:"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo ""
echo "🔍 Verify CDC Connector:"
echo "   curl http://localhost:8083/connectors"
echo ""
echo "📝 Next Steps:"
echo "   1. Run: ./scripts/start_pipeline.sh"
echo "   2. Wait 1-2 minutes for data collection"
echo "   3. Open: notebooks/analytics_demo.ipynb"
echo ""
