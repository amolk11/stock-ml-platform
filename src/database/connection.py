"""
Database Connection Manager - Thread-safe connection pooling
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager
from typing import Generator
import logging

from src.utils.config_loader import load_config

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages database connections with connection pooling."""
    
    _instance = None
    _engine = None
    _session_factory = None
    
    def __new__(cls):
        """Singleton pattern to ensure single connection pool."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def initialize(self, config_path: str = "configs/database.yaml"):
        """
        Initialize database connection pool.
        
        Args:
            config_path: Path to database configuration YAML
        """
        if self._engine is not None:
            logger.warning("Database already initialized")
            return
        
        config = load_config(config_path)
        db_config = config['database']
        
        # Build connection URL
        connection_url = (
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
            f"?charset=utf8mb4"
        )
        
        # Create engine with connection pooling
        self._engine = create_engine(
            connection_url,
            poolclass=QueuePool,
            pool_size=db_config.get('pool_size', 10),
            max_overflow=db_config.get('max_overflow', 20),
            pool_timeout=db_config.get('pool_timeout', 30),
            pool_recycle=db_config.get('pool_recycle', 3600),
            pool_pre_ping=True,  # Verify connections before using
            echo=db_config.get('echo', False)  # SQL logging
        )
        
        # Create session factory
        self._session_factory = sessionmaker(
            bind=self._engine,
            autocommit=False,
            autoflush=False
        )
        
        logger.info(
            f"Database initialized: {db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
    
    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """
        Context manager for database sessions.
        
        Usage:
            with db_manager.get_session() as session:
                result = session.query(Model).all()
        
        Yields:
            SQLAlchemy session
        """
        if self._session_factory is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")
        
        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        finally:
            session.close()
    
    def dispose(self):
        """Dispose of connection pool (use on application shutdown)."""
        if self._engine:
            self._engine.dispose()
            logger.info("Database connections disposed")


# Global instance
db_manager = DatabaseManager()