import os
import io
import time
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================================================
#                 DATABASE CONFIG
# =========================================================
DB_USER = os.getenv('DB_USER', 'shopzada_admin')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password123')
DB_HOST = os.getenv('DB_HOST', 'db')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'shopzada_dwh')

# Airflow container override
if os.path.exists('/opt/airflow'):
    DB_HOST = 'shopzada-postgres-db'

DB_URI = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# =========================================================
#                 TABLE CONFIG
# =========================================================
SOURCE_TABLES = ["raw_schema.user_data_json"]

STAGING_SCHEMA = "staging1_schema"
STAGING_TABLE = "user_data_cleaned"

REQUIRED_COLUMNS = [
    "user_id",
    "creation_date",
    "name",
    "street",
    "state",
    "city",
    "country",
    "birthdate",
    "gender",
    "device_address",
    "user_type"
]

DTYPE_MAPPING = {
    "user_id": "VARCHAR(9)",
    "creation_date": "DATE",
    "name": "VARCHAR(40)",
    "street": "VARCHAR(40)",
    "state": "VARCHAR(27)",
    "city": "VARCHAR(20)",
    "country": "VARCHAR(52)",
    "birthdate": "TIMESTAMP",
    "gender": "VARCHAR(6)",
    "device_address": "VARCHAR(17)",
    "user_type": "VARCHAR(8)",
    "ingested_at": "TIMESTAMP"
}

# =========================================================
#                GEOCODING SETUP WITH FALLBACKS
# =========================================================

def create_session():
    """Create a requests session with retry logic for Docker environments"""
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

# US State abbreviations to full names mapping
US_STATES = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
    'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
    'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
    'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
    'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
    'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
    'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
    'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
    'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
    'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
    'WI': 'Wisconsin', 'WY': 'Wyoming', 'DC': 'District of Columbia'
}