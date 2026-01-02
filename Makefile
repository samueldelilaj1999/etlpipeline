.PHONY: help install setup start stop health clean restart logs spark-ui test validate-env

# Default target
help:
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "  CDC PIPELINE - AVAILABLE COMMANDS"
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo ""
	@echo "  📦 SETUP:"
	@echo "  make install      - Install Python dependencies (PySpark, etc.)"
	@echo "  make setup        - Setup Docker infrastructure (PostgreSQL, Kafka, Debezium)"
	@echo ""
	@echo "  ▶️  START/STOP:"
	@echo "  make start        - Start Spark CDC pipeline locally"
	@echo "  make stop         - Stop all services (Docker + Spark)"
	@echo "  make restart      - Restart everything"
	@echo ""
	@echo "  🔍 MONITORING:"
	@echo "  make health       - Check health of all components"
	@echo "  make logs         - View Docker service logs"
	@echo "  make spark-ui     - Open Spark UI in browser (http://localhost:4040)"
	@echo ""
	@echo "  🛠️  UTILITIES:"
	@echo "  make validate-env - Validate environment configuration"
	@echo "  make clean        - Remove all generated data"
	@echo "  make test         - Run health check"
	@echo ""
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Install Python dependencies
install:
	@echo "📦 Installing Python dependencies..."
	@pip install -r requirements.txt
	@echo "✅ Dependencies installed"
	@echo ""
	@echo "💡 Next steps:"
	@echo "   1. make setup     - Start Docker infrastructure"
	@echo "   2. make start     - Start Spark CDC pipeline"

# Setup Docker infrastructure only (no Spark container)
setup:
	@echo "🚀 Setting up Docker infrastructure..."
	@bash scripts/setup_infrastructure.sh

# Validate environment configuration
validate-env:
	@echo "🔍 Validating environment configuration..."
	@python scripts/validate_env.py

# Start Spark CDC locally
start:
	@echo "⚡ Starting Spark CDC pipeline locally..."
	@echo "📝 Note: This will run in the foreground. Press Ctrl+C to stop."
	@echo "🌐 Spark UI will be available at: http://localhost:4040"
	@echo ""
	@bash scripts/start_pipeline.sh

# Stop all services
stop:
	@echo "🛑 Stopping all services..."
	@bash scripts/stop_pipeline.sh

# Health check
health:
	@echo "🏥 Running health check..."
	@bash scripts/health_check.sh

# Clean all data
clean:
	@echo "🧹 Cleaning up data..."
	@rm -rf /tmp/cdc_warehouse /tmp/spark_checkpoints /tmp/cdc_dead_letter
	@rm -rf warehouse/ checkpoints/
	@echo "✅ Data cleaned"

# Restart everything
restart: stop
	@echo "⏳ Waiting 5 seconds..."
	@sleep 5
	@echo "🔄 Restarting Docker services..."
	@docker-compose restart
	@echo "✅ Docker services restarted"
	@echo ""
	@echo "💡 Now run: make start"

# View Docker logs
logs:
	@echo "📋 Docker service logs (Ctrl+C to exit)..."
	@docker-compose logs -f

# Open Spark UI in browser
spark-ui:
	@echo "🌐 Opening Spark UI..."
	@open http://localhost:4040 || xdg-open http://localhost:4040 || echo "Open http://localhost:4040 in your browser"

# Quick test
test: health
	@echo ""
	@echo "✅ All checks passed!"
