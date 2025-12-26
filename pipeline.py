import pandas as pd
import os
from sqlalchemy import create_engine

def run_pipeline():
    # ElephantSQL Tiny Turtle connection string
    # Format: postgresql://user:password@host:port/database
    # Replace the string below with your actual ElephantSQL URL
    DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://user:pass@host/db')
    
    # SQLAlchemy requires the prefix to be postgresql://, but some providers give postgres://
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

    engine = create_engine(DATABASE_URL)
    
    # ...existing code...