"""
Environment Configuration Loader
=================================

Utility module for loading and managing environment variables across the CDC pipeline.

Usage:
    from config.env_loader import load_env, get_config, get_kafka_config, get_spark_config
    
    # Load environment variables from .env
    load_env()
    
    # Get configuration dictionaries
    kafka_config = get_kafka_config()
    spark_config = get_spark_config()
"""

import os
from pathlib import Path
from typing import Dict, Any, Optional


def load_env(env_file: str = '.env', override: bool = False) -> Dict[str, str]:
    """
    Load environment variables from a file.
    
    Args:
        env_file: Path to the environment file (default: .env)
        override: Whether to override existing environment variables
        
    Returns:
        Dictionary of loaded environment variables
    """
    env_path = Path(env_file)
    loaded_vars = {}
    
    if not env_path.exists():
        print(f"⚠️  Warning: {env_file} not found, using system environment variables")
        return loaded_vars
    
    with open(env_path, 'r') as f:
        for line in f:
            line = line.strip()
            # Skip comments and empty lines
            if not line or line.startswith('#'):
                continue
            
            if '=' in line:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip()
                
                # Remove quotes if present
                if value.startswith('"') and value.endswith('"'):
                    value = value[1:-1]
                elif value.startswith("'") and value.endswith("'"):
                    value = value[1:-1]
                
                # Set environment variable
                if override or key not in os.environ:
                    os.environ[key] = value
                    loaded_vars[key] = value
    
    print(f"✅ Loaded {len(loaded_vars)} variables from {env_file}")
    return loaded_vars


def get_config(key: str, default: Any = None, required: bool = False) -> Any:
    """
    Get a configuration value from environment.
    
    Args:
        key: Environment variable name
        default: Default value if not found
        required: Raise error if not found
        
    Returns:
        Configuration value
        
    Raises:
        ValueError: If required=True and variable not found
    """
    value = os.getenv(key, default)
    
    if required and value is None:
        raise ValueError(f"Required environment variable '{key}' not set")
    
    return value


def get_postgres_config() -> Dict[str, Any]:
    """Get PostgreSQL configuration"""
    return {
        'host': get_config('POSTGRES_HOST', 'postgres-source'),
        'port': int(get_config('POSTGRES_PORT', 5432)),
        'database': get_config('POSTGRES_DB', 'sourcedb'),
        'user': get_config('POSTGRES_USER', 'sourceuser'),
        'password': get_config('POSTGRES_PASSWORD', 'sourcepass'),
    }


def get_db_config() -> Dict[str, Any]:
    """Get database configuration for data generator"""
    return {
        'host': get_config('DB_HOST', 'localhost'),
        'port': int(get_config('DB_PORT', 5432)),
        'database': get_config('DB_NAME', 'sourcedb'),
        'user': get_config('DB_USER', 'sourceuser'),
        'password': get_config('DB_PASSWORD', 'sourcepass'),
    }


def get_kafka_config() -> Dict[str, Any]:
    """Get Kafka configuration"""
    return {
        'bootstrap_servers': get_config('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'),
        'broker_id': int(get_config('KAFKA_BROKER_ID', 1)),
        'zookeeper_connect': get_config('KAFKA_ZOOKEEPER_CONNECT', 'zookeeper:2181'),
        'auto_create_topics': get_config('KAFKA_AUTO_CREATE_TOPICS_ENABLE', 'true').lower() == 'true',
    }


def get_spark_config() -> Dict[str, Any]:
    """Get Spark CDC application configuration"""
    return {
        'kafka_bootstrap_servers': get_config('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'),
        'checkpoint_location': get_config('CHECKPOINT_LOCATION', '/tmp/spark_checkpoints'),
        'warehouse_location': get_config('WAREHOUSE_LOCATION', '/tmp/cdc_warehouse'),
        'dlq_location': get_config('DLQ_LOCATION', '/tmp/cdc_dead_letter'),
        'app_name': get_config('SPARK_APP_NAME', 'ProductionCDCPipeline'),
        'master': get_config('SPARK_MASTER', 'local[*]'),
        'driver_memory': get_config('SPARK_DRIVER_MEMORY', '2g'),
        'executor_memory': get_config('SPARK_EXECUTOR_MEMORY', '2g'),
        'processing_interval': int(get_config('SPARK_PROCESSING_INTERVAL', 30)),
        'max_offsets_per_trigger': int(get_config('SPARK_MAX_OFFSETS_PER_TRIGGER', 1000)),
    }


def get_generator_config() -> Dict[str, Any]:
    """Get data generator configuration"""
    return {
        'mode': get_config('GENERATION_MODE', 'continuous'),
        'interval': int(get_config('GENERATION_INTERVAL', 5)),
        'initial_users': int(get_config('INITIAL_USERS', 100)),
        'initial_orders': int(get_config('INITIAL_ORDERS', 500)),
        'initial_devices': int(get_config('INITIAL_DEVICES', 150)),
    }


def get_logging_config() -> Dict[str, str]:
    """Get logging configuration"""
    return {
        'level': get_config('LOG_LEVEL', 'INFO'),
        'kafka_level': get_config('KAFKA_LOG_LEVEL', 'WARN'),
        'spark_level': get_config('SPARK_LOG_LEVEL', 'WARN'),
        'zookeeper_level': get_config('ZOOKEEPER_LOG_LEVEL', 'WARN'),
    }


def print_config_summary():
    """Print a summary of current configuration"""
    print("\n" + "="*60)
    print("📋 CONFIGURATION SUMMARY")
    print("="*60)
    
    print("\n🗄️  PostgreSQL:")
    pg_config = get_postgres_config()
    for key, value in pg_config.items():
        if key == 'password':
            print(f"  {key:15} = {'*' * len(str(value))}")
        else:
            print(f"  {key:15} = {value}")
    
    print("\n📡 Kafka:")
    kafka_config = get_kafka_config()
    for key, value in kafka_config.items():
        print(f"  {key:20} = {value}")
    
    print("\n⚡ Spark:")
    spark_config = get_spark_config()
    for key, value in spark_config.items():
        print(f"  {key:25} = {value}")
    
    print("\n🔄 Data Generator:")
    gen_config = get_generator_config()
    for key, value in gen_config.items():
        print(f"  {key:15} = {value}")
    
    print("\n📊 Logging:")
    log_config = get_logging_config()
    for key, value in log_config.items():
        print(f"  {key:15} = {value}")
    
    print("\n" + "="*60 + "\n")


# Auto-load .env when module is imported (can be disabled)
AUTO_LOAD = os.getenv('ENV_AUTO_LOAD', 'true').lower() == 'true'

if AUTO_LOAD:
    # Try to find .env in current directory or parent directories
    current_dir = Path.cwd()
    env_file = None
    
    for parent in [current_dir] + list(current_dir.parents):
        potential_env = parent / '.env'
        if potential_env.exists():
            env_file = str(potential_env)
            break
    
    if env_file:
        load_env(env_file)


if __name__ == "__main__":
    # When run directly, print configuration summary
    print_config_summary()
