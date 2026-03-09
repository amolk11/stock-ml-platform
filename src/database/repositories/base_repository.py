"""
Base Repository Pattern - Abstract data access layer
"""

from typing import List, Optional, Dict, Any, TypeVar, Generic
from sqlalchemy.orm import Session
from abc import ABC, abstractmethod

T = TypeVar('T')


class BaseRepository(ABC, Generic[T]):
    """
    Abstract base repository providing common CRUD operations.
    
    This implements the Repository Pattern to abstract database access.
    """
    
    def __init__(self, session: Session, model_class: type):
        """
        Initialize repository.
        
        Args:
            session: SQLAlchemy session
            model_class: ORM model class
        """
        self.session = session
        self.model_class = model_class
    
    def create(self, **kwargs) -> T:
        """Create a new record."""
        instance = self.model_class(**kwargs)
        self.session.add(instance)
        self.session.flush()
        return instance
    
    def get_by_id(self, record_id: int) -> Optional[T]:
        """Get record by primary key."""
        return self.session.query(self.model_class).filter(
            self.model_class.id == record_id
        ).first()
    
    def get_all(self, limit: Optional[int] = None) -> List[T]:
        """Get all records with optional limit."""
        query = self.session.query(self.model_class)
        if limit:
            query = query.limit(limit)
        return query.all()
    
    def update(self, record_id: int, **kwargs) -> Optional[T]:
        """Update a record by ID."""
        instance = self.get_by_id(record_id)
        if instance:
            for key, value in kwargs.items():
                setattr(instance, key, value)
            self.session.flush()
        return instance
    
    def delete(self, record_id: int) -> bool:
        """Delete a record by ID."""
        instance = self.get_by_id(record_id)
        if instance:
            self.session.delete(instance)
            self.session.flush()
            return True
        return False
    
    def bulk_insert(self, records: List[Dict[str, Any]]) -> int:
        """
        Bulk insert records efficiently.
        
        Args:
            records: List of dictionaries containing record data
            
        Returns:
            Number of records inserted
        """
        if not records:
            return 0
        
        self.session.bulk_insert_mappings(self.model_class, records)
        self.session.flush()
        return len(records)