"""
Data Models for CDC Pipeline Generator
Defines domain entities with proper validation and structure
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from decimal import Decimal


@dataclass
class User:
    """User entity representing a customer in the system"""
    username: str
    email: str
    full_name: str
    status: str = 'active'
    user_id: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    def to_tuple(self) -> tuple:
        """Convert to tuple for database insertion"""
        return (self.username, self.email, self.full_name, self.status)


@dataclass
class Order:
    """Order entity representing a transaction"""
    user_id: int
    order_number: str
    status: str
    total_amount: Decimal
    currency: str = 'USD'
    order_id: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    def to_tuple(self) -> tuple:
        """Convert to tuple for database insertion"""
        return (self.user_id, self.order_number, self.status, float(self.total_amount), self.currency)


@dataclass
class Device:
    """Device entity representing a user's connected device"""
    user_id: int
    device_type: str
    device_name: str
    os_version: str
    app_version: str
    is_active: bool = True
    device_id: Optional[int] = None
    registered_at: Optional[datetime] = None
    last_active: Optional[datetime] = None
    
    def to_tuple(self) -> tuple:
        """Convert to tuple for database insertion"""
        return (self.user_id, self.device_type, self.device_name, 
                self.os_version, self.app_version, self.is_active)


# Enums for status values
class UserStatus:
    ACTIVE = 'active'
    INACTIVE = 'inactive'
    SUSPENDED = 'suspended'
    
    @classmethod
    def all(cls):
        return [cls.ACTIVE, cls.INACTIVE, cls.SUSPENDED]


class OrderStatus:
    PENDING = 'pending'
    PROCESSING = 'processing'
    SHIPPED = 'shipped'
    DELIVERED = 'delivered'
    CANCELLED = 'cancelled'
    
    @classmethod
    def all(cls):
        return [cls.PENDING, cls.PROCESSING, cls.SHIPPED, cls.DELIVERED, cls.CANCELLED]


class DeviceType:
    IOS = 'iOS'
    ANDROID = 'Android'
    WEB = 'Web'
    DESKTOP = 'Desktop'
    TABLET = 'Tablet'
    
    @classmethod
    def all(cls):
        return [cls.IOS, cls.ANDROID, cls.WEB, cls.DESKTOP, cls.TABLET]
