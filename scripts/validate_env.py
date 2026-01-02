#!/usr/bin/env python3
"""
Environment Configuration Validator
====================================

Validates that all required environment variables are set correctly.
Run this before starting the pipeline to catch configuration issues early.

Usage:
    python scripts/validate_env.py
    python scripts/validate_env.py --env-file .env.production
"""

import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple


class Colors:
    """ANSI color codes"""
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    END = '\033[0m'


def print_header(text: str):
    """Print formatted header"""
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*60}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{text}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'='*60}{Colors.END}\n")


def print_success(text: str):
    """Print success message"""
    print(f"{Colors.GREEN}✅ {text}{Colors.END}")


def print_warning(text: str):
    """Print warning message"""
    print(f"{Colors.YELLOW}⚠️  {text}{Colors.END}")


def print_error(text: str):
    """Print error message"""
    print(f"{Colors.RED}❌ {text}{Colors.END}")


def load_env_file(env_file: str = '.env') -> Dict[str, str]:
    """Load environment variables from file"""
    env_vars = {}
    env_path = Path(env_file)
    
    if not env_path.exists():
        return env_vars
    
    with open(env_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                env_vars[key.strip()] = value.strip()
    
    return env_vars


# Define required environment variables and their validation rules
REQUIRED_VARS = {
    'PostgreSQL': [
        ('POSTGRES_HOST', str, 'postgres-source'),
        ('POSTGRES_PORT', int, 5432),
        ('POSTGRES_DB', str, 'sourcedb'),
        ('POSTGRES_USER', str, 'sourceuser'),
        ('POSTGRES_PASSWORD', str, 'sourcepass'),
    ],
    'Data Generator': [
        ('DB_HOST', str, 'postgres-source'),
        ('DB_PORT', int, 5432),
        ('DB_NAME', str, 'sourcedb'),
        ('DB_USER', str, 'sourceuser'),
        ('DB_PASSWORD', str, 'sourcepass'),
        ('GENERATION_MODE', str, 'continuous'),
        ('GENERATION_INTERVAL', int, 5),
    ],
    'Kafka': [
        ('KAFKA_BOOTSTRAP_SERVERS', str, 'kafka:29092'),
        ('KAFKA_BROKER_ID', int, 1),
        ('KAFKA_ZOOKEEPER_CONNECT', str, 'zookeeper:2181'),
    ],
    'Spark CDC': [
        ('KAFKA_BOOTSTRAP_SERVERS', str, 'kafka:29092'),
        ('CHECKPOINT_LOCATION', str, '/tmp/spark_checkpoints'),
        ('WAREHOUSE_LOCATION', str, '/tmp/cdc_warehouse'),
        ('DLQ_LOCATION', str, '/tmp/cdc_dead_letter'),
    ],
}


def validate_variable(name: str, var_type: type, default: any, env_vars: Dict[str, str]) -> Tuple[bool, str]:
    """Validate a single environment variable"""
    value = env_vars.get(name, os.getenv(name))
    
    if value is None:
        return False, f"Not set (default: {default})"
    
    # Type validation
    try:
        if var_type == int:
            int(value)
        elif var_type == bool:
            if value.lower() not in ['true', 'false', '1', '0', 'yes', 'no']:
                return False, f"Invalid boolean: {value}"
        # Additional validations
        if name.endswith('_PORT'):
            port = int(value)
            if not (1 <= port <= 65535):
                return False, f"Invalid port: {port}"
        
        if name.endswith('_PASSWORD') and len(value) < 8:
            return False, f"Password too short (< 8 characters)"
        
        if name.endswith('_LOCATION') or name.endswith('_DIR'):
            # Check if path is valid (but don't require it to exist yet)
            if not value or value.isspace():
                return False, "Empty path"
        
        return True, value
    except ValueError as e:
        return False, f"Type error: expected {var_type.__name__}, got {value}"


def validate_environment(env_file: str = '.env') -> Tuple[int, int, int]:
    """Validate all environment variables"""
    print_header("🔍 ENVIRONMENT CONFIGURATION VALIDATOR")
    
    # Load .env file
    env_vars = load_env_file(env_file)
    
    if env_vars:
        print_success(f"Loaded {len(env_vars)} variables from {env_file}")
    else:
        print_warning(f"{env_file} not found or empty, checking system environment")
    
    total = 0
    valid = 0
    warnings = 0
    errors = 0
    
    # Validate each category
    for category, variables in REQUIRED_VARS.items():
        print(f"\n{Colors.BOLD}📋 {category}{Colors.END}")
        
        for var_name, var_type, default in variables:
            total += 1
            is_valid, message = validate_variable(var_name, var_type, default, env_vars)
            
            if is_valid:
                valid += 1
                print_success(f"{var_name:35} = {message}")
            elif "Not set" in message:
                warnings += 1
                print_warning(f"{var_name:35} {message}")
            else:
                errors += 1
                print_error(f"{var_name:35} {message}")
    
    # Validation summary
    print_header("📊 VALIDATION SUMMARY")
    
    print(f"Total Variables: {total}")
    print_success(f"Valid: {valid}")
    
    if warnings > 0:
        print_warning(f"Using Defaults: {warnings}")
    
    if errors > 0:
        print_error(f"Errors: {errors}")
    
    return total, valid, errors


def check_directories():
    """Check if required directories exist or can be created"""
    print_header("📁 DIRECTORY CHECK")
    
    dirs_to_check = [
        os.getenv('CHECKPOINT_LOCATION', '/tmp/spark_checkpoints'),
        os.getenv('WAREHOUSE_LOCATION', '/tmp/cdc_warehouse'),
        os.getenv('DLQ_LOCATION', '/tmp/cdc_dead_letter'),
    ]
    
    for dir_path in dirs_to_check:
        try:
            path = Path(dir_path)
            if path.exists():
                print_success(f"{dir_path:35} exists")
            else:
                # Try to create
                path.mkdir(parents=True, exist_ok=True)
                print_success(f"{dir_path:35} created")
        except Exception as e:
            print_error(f"{dir_path:35} cannot create: {e}")


def check_network_connectivity():
    """Check if Docker network exists"""
    print_header("🌐 NETWORK CHECK")
    
    import subprocess
    
    network_name = os.getenv('NETWORK_NAME', 'cdc-network')
    
    try:
        result = subprocess.run(
            ['docker', 'network', 'ls', '--format', '{{.Name}}'],
            capture_output=True,
            text=True,
            check=True
        )
        
        networks = result.stdout.strip().split('\n')
        if network_name in networks:
            print_success(f"Docker network '{network_name}' exists")
        else:
            print_warning(f"Docker network '{network_name}' not found (will be created)")
    except subprocess.CalledProcessError:
        print_warning("Could not check Docker networks (Docker not running?)")
    except FileNotFoundError:
        print_warning("Docker command not found")


def main():
    """Main validation function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Validate environment configuration')
    parser.add_argument('--env-file', default='.env', help='Environment file to validate')
    parser.add_argument('--skip-checks', action='store_true', help='Skip directory and network checks')
    args = parser.parse_args()
    
    # Validate environment variables
    total, valid, errors = validate_environment(args.env_file)
    
    # Additional checks
    if not args.skip_checks:
        check_directories()
        check_network_connectivity()
    
    # Final result
    print_header("🎯 FINAL RESULT")
    
    if errors > 0:
        print_error(f"Validation FAILED with {errors} error(s)")
        print(f"\n{Colors.YELLOW}Please fix the errors before starting the pipeline.{Colors.END}")
        print(f"{Colors.YELLOW}See docs/ENVIRONMENT_CONFIG.md for details.{Colors.END}\n")
        sys.exit(1)
    elif valid == total:
        print_success("All validations PASSED!")
        print(f"\n{Colors.GREEN}✨ Your environment is properly configured!{Colors.END}")
        print(f"{Colors.GREEN}You can now start the pipeline.{Colors.END}\n")
        sys.exit(0)
    else:
        print_warning(f"Using default values for {total - valid} variable(s)")
        print(f"\n{Colors.YELLOW}Configuration is valid but using defaults.{Colors.END}")
        print(f"{Colors.YELLOW}Consider creating a .env file for custom configuration.{Colors.END}\n")
        sys.exit(0)


if __name__ == "__main__":
    main()
