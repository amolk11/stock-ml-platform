"""
Database Setup Script - Initialize MySQL database schema
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import logging
from sqlalchemy import create_engine, text
from src.utils.config_loader import load_config
from src.utils.logger import LoggerFactory
from src.database.models import Base

# Setup logging
LoggerFactory.setup_logging(log_level="INFO")
logger = logging.getLogger(__name__)


def create_database_if_not_exists(config: dict):
    """Create database if it doesn't exist."""
    db_config = config['database']
    
    # Connect without database specified
    connection_url = (
        f"mysql+pymysql://{db_config['user']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}"
    )
    
    engine = create_engine(connection_url)
    
    with engine.connect() as conn:
        # Check if database exists
        result = conn.execute(
            text(f"SHOW DATABASES LIKE '{db_config['database']}'")
        )
        
        if not result.fetchone():
            conn.execute(text(f"CREATE DATABASE {db_config['database']}"))
            conn.commit()
            logger.info(f"Created database: {db_config['database']}")
        else:
            logger.info(f"Database already exists: {db_config['database']}")
    
    engine.dispose()


def create_tables(config: dict):
    """Create all tables defined in ORM models."""
    db_config = config['database']
    
    connection_url = (
        f"mysql+pymysql://{db_config['user']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    )
    
    engine = create_engine(connection_url, echo=True)
    
    # Create all tables
    Base.metadata.create_all(engine)
    logger.info("All tables created successfully")
    
    engine.dispose()


def main():
    """Main setup function."""
    try:
        logger.info("Starting database setup...")
        
        # Load configuration
        config = load_config("configs/database.yaml")
        
        # Step 1: Create database
        create_database_if_not_exists(config)
        
        # Step 2: Create tables
        create_tables(config)
        
        logger.info("Database setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Database setup failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()