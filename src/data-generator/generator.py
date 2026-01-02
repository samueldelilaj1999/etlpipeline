#!/usr/bin/env python3
"""
Data Generator for CDC Pipeline
Clean architecture with proper models, factory, and repository pattern
"""

import os
import sys
import time
import random
import logging
from typing import Dict, Any

import psycopg2

from models import UserStatus, OrderStatus
from factory import EntityFactory
from repository import DatabaseRepository

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'sourcedb'),
    'user': os.getenv('DB_USER', 'sourceuser'),
    'password': os.getenv('DB_PASSWORD', 'sourcepass')
}

GENERATION_MODE = os.getenv('GENERATION_MODE', 'continuous')
GENERATION_INTERVAL = int(os.getenv('GENERATION_INTERVAL', 5))
INITIAL_USERS = int(os.getenv('INITIAL_USERS', 100))
INITIAL_ORDERS = int(os.getenv('INITIAL_ORDERS', 500))
INITIAL_DEVICES = int(os.getenv('INITIAL_DEVICES', 150))


class DataGeneratorService:
    """Service for orchestrating data generation using clean architecture"""
    
    def __init__(self, db_config: Dict[str, Any]):
        self.db_config = db_config
        self.conn = None
        self.repository = None
        self.factory = EntityFactory()
        
    def connect(self) -> bool:
        """Establish database connection with retry logic"""
        max_retries = 10
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                self.conn = psycopg2.connect(**self.db_config)
                self.conn.autocommit = False
                self.repository = DatabaseRepository(self.conn)
                logger.info(f" Connected to database at {self.db_config['host']}")
                return True
            except psycopg2.OperationalError as e:
                logger.warning(f"Connection attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    logger.error("Failed to connect to database after all retries")
                    return False
        return False
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def initial_load(self):
        """Perform initial data load"""
        logger.info("🚀 Starting initial data load...")
        
        # Generate and insert users
        users = self.factory.create_users(INITIAL_USERS)
        user_ids = self.repository.insert_users(users)
        
        if not user_ids:
            logger.error("No users created, cannot continue")
            return
        
        logger.info(f" Created {len(user_ids)} users")
        
        # Generate and insert orders
        orders = self.factory.create_orders(user_ids, INITIAL_ORDERS)
        self.repository.insert_orders(orders)
        
        # Generate and insert devices
        devices = self.factory.create_devices(user_ids, INITIAL_DEVICES)
        self.repository.insert_devices(devices)
        
        logger.info(" Initial data load completed")
        self.print_stats()
    
    def continuous_generation(self):
        """Continuously generate new data"""
        logger.info(f"🔄 Starting continuous data generation (interval: {GENERATION_INTERVAL}s)")
        
        while True:
            try:
                user_ids = self.repository.get_all_user_ids()
                
                if not user_ids:
                    logger.warning("No users found, performing initial load")
                    self.initial_load()
                    user_ids = self.repository.get_all_user_ids()
                
                # Randomly decide what to generate (weighted distribution)
                action = random.choice(
                    ['new_user'] * 1 + 
                    ['new_order'] * 5 + 
                    ['new_device'] * 2 + 
                    ['update'] * 3
                )
                
                if action == 'new_user':
                    count = random.randint(1, 3)
                    users = self.factory.create_users(count)
                    new_ids = self.repository.insert_users(users)
                    user_ids.extend(new_ids)
                    
                elif action == 'new_order':
                    count = random.randint(1, 10)
                    orders = self.factory.create_orders(user_ids, count)
                    self.repository.insert_orders(orders)
                    
                elif action == 'new_device':
                    count = random.randint(1, 5)
                    devices = self.factory.create_devices(user_ids, count)
                    self.repository.insert_devices(devices)
                    
                elif action == 'update':
                    self.update_random_records()
                
                time.sleep(GENERATION_INTERVAL)
                
            except KeyboardInterrupt:
                logger.info("⏹️  Stopping data generation")
                break
            except Exception as e:
                logger.error(f"Error in continuous generation: {e}")
                time.sleep(GENERATION_INTERVAL)
    
    def update_random_records(self):
        """Simulate updates to existing records"""
        try:
            # Update random order statuses
            status = random.choice(OrderStatus.all())
            self.repository.update_random_orders(status, limit=5)
            
            # Update random user statuses
            status = random.choice(UserStatus.all())
            self.repository.update_random_users(status, limit=3)
            
            # Update random device activity
            self.repository.update_random_devices_activity(limit=10)
            
            logger.info(" Updated random records")
            
        except Exception as e:
            logger.error(f"Error updating records: {e}")
    
    def print_stats(self):
        """Print current database statistics"""
        try:
            stats = self.repository.get_statistics()
            
            logger.info("=" * 60)
            logger.info("📊 DATABASE STATISTICS")
            logger.info(f"   Users:   {stats['users']:,}")
            logger.info(f"   Orders:  {stats['orders']:,}")
            logger.info(f"   Devices: {stats['devices']:,}")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"Error getting stats: {e}")


def main():
    """Main entry point"""
    logger.info("🎬 Data Generator Starting...")
    logger.info(f"   Mode: {GENERATION_MODE}")
    logger.info(f"   Database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    
    service = DataGeneratorService(DB_CONFIG)
    
    if not service.connect():
        logger.error("Failed to connect to database, exiting")
        sys.exit(1)
    
    try:
        if GENERATION_MODE == 'initial':
            service.initial_load()
        else:
            # Do initial load if tables are empty
            user_ids = service.repository.get_all_user_ids()
            if not user_ids:
                service.initial_load()
            
            service.continuous_generation()
            
    except KeyboardInterrupt:
        logger.info("⏹️  Interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        service.close()
        logger.info("👋 Data Generator Stopped")


if __name__ == "__main__":
    main()
