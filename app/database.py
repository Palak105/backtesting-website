from sqlalchemy import create_engine
import os
from pathlib import Path

# Use environment variable or default to project root
DB_URL = os.getenv("DATABASE_URL")


engine = create_engine(
    DB_URL,
    connect_args={"check_same_thread": False}
)