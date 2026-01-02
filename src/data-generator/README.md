# Data Generator Service

Generates realistic sample data for the CDC pipeline source database.

## Features

- 📊 Generates realistic data for users, orders, and devices
- 🔄 Continuous data generation with configurable intervals
- ♻️ Simulates updates to existing records
- 🎲 Uses Faker library for realistic data
- 🔌 Automatic database connection with retry logic

## Data Models

### Users
- username, email, full_name
- status: active, inactive, suspended
- timestamps: created_at, updated_at

### Orders
- user_id (foreign key)
- order_number, status, total_amount, currency
- timestamps: created_at, updated_at

### Devices
- user_id (foreign key)
- device_type (iOS, Android, Web, Desktop, Tablet)
- device_name, os_version, app_version
- is_active flag
- timestamps: registered_at, last_active

## Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=sourcedb
export DB_USER=sourceuser
export DB_PASSWORD=sourcepass
export GENERATION_MODE=continuous
export GENERATION_INTERVAL=5

# Run generator
python generator.py
```

## Running with Docker

```bash
# Build and run with docker-compose
cd ../../infra/compose
docker-compose -f docker-compose.source.yml up -d
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| DB_HOST | localhost | Database host |
| DB_PORT | 5432 | Database port |
| DB_NAME | sourcedb | Database name |
| DB_USER | sourceuser | Database user |
| DB_PASSWORD | sourcepass | Database password |
| GENERATION_MODE | continuous | 'initial' or 'continuous' |
| GENERATION_INTERVAL | 5 | Seconds between generations |
| INITIAL_USERS | 100 | Initial user count |
| INITIAL_ORDERS | 500 | Initial order count |
| INITIAL_DEVICES | 150 | Initial device count |

## Generation Modes

### Initial Mode
- Loads initial dataset
- Exits after completion

### Continuous Mode
- Performs initial load if tables are empty
- Continuously generates new data
- Simulates updates to existing records
- Runs until stopped
