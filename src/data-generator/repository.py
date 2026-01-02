"""
Database Repository Layer
Handles all database operations with proper separation of concerns
"""

import logging
from typing import List, Optional
from psycopg2.extras import execute_batch
import psycopg2

from models import User, Order, Device

logger = logging.getLogger(__name__)


class DatabaseRepository:
    """Repository pattern for database operations"""
    
    def __init__(self, connection):
        self.conn = connection
    
    # ===================== USER OPERATIONS =====================
    
    def insert_users(self, users: List[User]) -> List[int]:
        """Insert users and return their IDs"""
        try:
            cur = self.conn.cursor()
            query = """
                INSERT INTO users (username, email, full_name, status)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (username) DO NOTHING
                RETURNING user_id
            """
            user_tuples = [user.to_tuple() for user in users]
            execute_batch(cur, query, user_tuples)
            self.conn.commit()
            
            # Get all user IDs
            cur.execute("SELECT user_id FROM users")
            user_ids = [row[0] for row in cur.fetchall()]
            cur.close()
            
            logger.info(f"✅ Inserted {len(users)} users")
            return user_ids
        except Exception as e:
            self.conn.rollback()
            logger.error(f"❌ Error inserting users: {e}")
            return []
    
    def get_all_user_ids(self) -> List[int]:
        """Get all user IDs from database"""
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT user_id FROM users")
            user_ids = [row[0] for row in cur.fetchall()]
            cur.close()
            return user_ids
        except Exception as e:
            logger.error(f"❌ Error getting user IDs: {e}")
            return []
    
    def update_random_users(self, status: str, limit: int = 3) -> int:
        """Update random user statuses"""
        try:
            cur = self.conn.cursor()
            cur.execute("""
                UPDATE users 
                SET status = %s, updated_at = CURRENT_TIMESTAMP
                WHERE user_id IN (
                    SELECT user_id FROM users 
                    ORDER BY RANDOM() 
                    LIMIT %s
                )
            """, (status, limit))
            affected = cur.rowcount
            self.conn.commit()
            cur.close()
            return affected
        except Exception as e:
            self.conn.rollback()
            logger.error(f"❌ Error updating users: {e}")
            return 0
    
    def get_user_count(self) -> int:
        """Get total number of users"""
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT COUNT(*) FROM users")
            count = cur.fetchone()[0]
            cur.close()
            return count
        except Exception as e:
            logger.error(f"❌ Error getting user count: {e}")
            return 0
    
    # ===================== ORDER OPERATIONS =====================
    
    def insert_orders(self, orders: List[Order]) -> int:
        """Insert orders"""
        try:
            cur = self.conn.cursor()
            query = """
                INSERT INTO orders (user_id, order_number, status, total_amount, currency)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (order_number) DO NOTHING
            """
            order_tuples = [order.to_tuple() for order in orders]
            execute_batch(cur, query, order_tuples)
            self.conn.commit()
            affected = cur.rowcount
            cur.close()
            
            logger.info(f"✅ Inserted {affected} orders")
            return affected
        except Exception as e:
            self.conn.rollback()
            logger.error(f"❌ Error inserting orders: {e}")
            logger.error(f"   Details: {str(e)}")
            return 0
    
    def update_random_orders(self, status: str, limit: int = 5) -> int:
        """Update random order statuses"""
        try:
            cur = self.conn.cursor()
            cur.execute("""
                UPDATE orders 
                SET status = %s, updated_at = CURRENT_TIMESTAMP
                WHERE order_id IN (
                    SELECT order_id FROM orders 
                    WHERE status NOT IN ('delivered', 'cancelled')
                    ORDER BY RANDOM() 
                    LIMIT %s
                )
            """, (status, limit))
            affected = cur.rowcount
            self.conn.commit()
            cur.close()
            return affected
        except Exception as e:
            self.conn.rollback()
            logger.error(f"❌ Error updating orders: {e}")
            return 0
    
    def get_order_count(self) -> int:
        """Get total number of orders"""
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT COUNT(*) FROM orders")
            count = cur.fetchone()[0]
            cur.close()
            return count
        except Exception as e:
            logger.error(f"❌ Error getting order count: {e}")
            return 0
    
    # ===================== DEVICE OPERATIONS =====================
    
    def insert_devices(self, devices: List[Device]) -> int:
        """Insert devices"""
        try:
            cur = self.conn.cursor()
            query = """
                INSERT INTO devices (user_id, device_type, device_name, os_version, app_version, is_active)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            device_tuples = [device.to_tuple() for device in devices]
            execute_batch(cur, query, device_tuples)
            self.conn.commit()
            affected = cur.rowcount
            cur.close()
            
            logger.info(f"✅ Inserted {affected} devices")
            return affected
        except Exception as e:
            self.conn.rollback()
            logger.error(f"❌ Error inserting devices: {e}")
            logger.error(f"   Details: {str(e)}")
            return 0
    
    def update_random_devices_activity(self, limit: int = 10) -> int:
        """Update last_active timestamp for random active devices"""
        try:
            cur = self.conn.cursor()
            cur.execute("""
                UPDATE devices 
                SET last_active = CURRENT_TIMESTAMP
                WHERE device_id IN (
                    SELECT device_id FROM devices 
                    WHERE is_active = true
                    ORDER BY RANDOM() 
                    LIMIT %s
                )
            """, (limit,))
            affected = cur.rowcount
            self.conn.commit()
            cur.close()
            return affected
        except Exception as e:
            self.conn.rollback()
            logger.error(f"❌ Error updating devices: {e}")
            return 0
    
    def get_device_count(self) -> int:
        """Get total number of devices"""
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT COUNT(*) FROM devices")
            count = cur.fetchone()[0]
            cur.close()
            return count
        except Exception as e:
            logger.error(f"❌ Error getting device count: {e}")
            return 0
    
    # ===================== STATISTICS =====================
    
    def get_statistics(self) -> dict:
        """Get comprehensive database statistics"""
        return {
            'users': self.get_user_count(),
            'orders': self.get_order_count(),
            'devices': self.get_device_count()
        }
