from datetime import date, timedelta
import os
import pandas as pd
from sqlalchemy import create_engine, text

# --------------------------------------------------
# DATABASE CONFIG
# --------------------------------------------------
DB_USER = os.getenv("DB_USER", "shopzada_admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password123")
DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "shopzada_dw")

# Airflow container override
if os.path.exists("/opt/airflow"):
    DB_HOST = "shopzada-postgres-db"

ENGINE = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------
START_DATE = date(2020, 1, 1)
END_DATE = date(2030, 12, 31)

FIXED_HOLIDAYS = {
    (1, 1): "New Year's Day",
    (5, 1): "International Workers' Day",
    (12, 25): "Christmas Day",
}

# --------------------------------------------------
# GENERATE DATE DIMENSION
# --------------------------------------------------
rows = []
current_date = START_DATE

while current_date <= END_DATE:
    year = current_date.year
    month = current_date.month
    day = current_date.day

    holiday_name = FIXED_HOLIDAYS.get((month, day))
    is_holiday = holiday_name is not None

    rows.append({
        "date_sk": int(current_date.strftime("%Y%m%d")),
        "full_date": current_date,
        "day": day,
        "day_name": current_date.strftime("%A"),
        "day_of_week": current_date.isoweekday(),
        "week_of_year": current_date.isocalendar()[1],
        "month": month,
        "month_name": current_date.strftime("%B"),
        "quarter": (month - 1) // 3 + 1,
        "year": year,
        "is_weekend": current_date.weekday() >= 5,
        "is_holiday": is_holiday,
        "holiday_name": holiday_name
    })

    current_date += timedelta(days=1)

# --------------------------------------------------
# CREATE DATAFRAME
# --------------------------------------------------
dim_date = pd.DataFrame(rows)

# --------------------------------------------------
# DROP TABLE IF EXISTS (SAFE)
# --------------------------------------------------
with ENGINE.begin() as conn:
    conn.execute(
        text("DROP TABLE IF EXISTS star_schema.dim_date CASCADE;")
    )

# --------------------------------------------------
# WRITE TO STAR SCHEMA
# --------------------------------------------------
dim_date.to_sql(
    name="dim_date",
    schema="star_schema",
    con=ENGINE,
    if_exists="replace",
    index=False
)

print("✅ dim_date successfully loaded into star_schema.dim_date")