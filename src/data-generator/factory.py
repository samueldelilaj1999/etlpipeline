"""
Factory for generating realistic test data
Uses Faker library to create domain entities
"""

import random
from decimal import Decimal
from typing import List
from faker import Faker

from models import User, Order, Device, UserStatus, OrderStatus, DeviceType

fake = Faker()


class EntityFactory:
    """Factory for creating realistic test entities"""
    
    @staticmethod
    def create_user(status: str = None) -> User:
        """Create a single user with realistic data"""
        if status is None:
            # 75% active, 25% other statuses
            status = random.choice([UserStatus.ACTIVE] * 3 + [UserStatus.INACTIVE])
        
        username = fake.user_name() + str(random.randint(1000, 9999))
        email = fake.email()
        full_name = fake.name()
        
        return User(
            username=username,
            email=email,
            full_name=full_name,
            status=status
        )
    
    @staticmethod
    def create_users(count: int) -> List[User]:
        """Create multiple users"""
        return [EntityFactory.create_user() for _ in range(count)]
    
    @staticmethod
    def create_order(user_id: int, status: str = None) -> Order:
        """Create a single order with realistic data"""
        if status is None:
            status = random.choice(OrderStatus.all())
        
        order_number = f"ORD-{fake.unique.random_number(digits=10)}"
        total_amount = Decimal(str(round(random.uniform(10.0, 1000.0), 2)))
        
        return Order(
            user_id=user_id,
            order_number=order_number,
            status=status,
            total_amount=total_amount,
            currency='USD'
        )
    
    @staticmethod
    def create_orders(user_ids: List[int], count: int) -> List[Order]:
        """Create multiple orders for random users"""
        return [EntityFactory.create_order(random.choice(user_ids)) for _ in range(count)]
    
    @staticmethod
    def create_device(user_id: int, device_type: str = None) -> Device:
        """Create a single device with realistic data"""
        if device_type is None:
            device_type = random.choice(DeviceType.all())
        
        device_name = f"{fake.company()} {device_type}"
        os_version = f"{random.randint(10, 15)}.{random.randint(0, 9)}"
        app_version = f"{random.randint(1, 5)}.{random.randint(0, 20)}.{random.randint(0, 9)}"
        is_active = random.choice([True] * 3 + [False])  # 75% active
        
        return Device(
            user_id=user_id,
            device_type=device_type,
            device_name=device_name,
            os_version=os_version,
            app_version=app_version,
            is_active=is_active
        )
    
    @staticmethod
    def create_devices(user_ids: List[int], count: int) -> List[Device]:
        """Create multiple devices for random users"""
        return [EntityFactory.create_device(random.choice(user_ids)) for _ in range(count)]
