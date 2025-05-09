import random
import logging
import duckdb
from faker import Faker
from datetime import date, datetime, timedelta

logging.basicConfig(
    level=logging.INFO,                    
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler()]
)

DUCKDB_PATH = "/opt/airflow/data/warehouse.duckdb"

def _create_data(locale: str) -> Faker:
    logging.info(f"Creating synthetic data for {locale.split('_')[-1]} country code.")
    return Faker(locale)

def _generate_record(fake: Faker) -> tuple:
    person_name = fake.name()
    user_name = person_name.replace(" ", "").lower()
    email = f"{user_name}@{fake.free_email_domain()}"
    personal_number = fake.ssn()
    birth_date = fake.date_of_birth()
    address = fake.address().replace("\n", ", ")
    phone_number = fake.phone_number()
    mac_address = fake.mac_address()
    ip_address = fake.ipv4()
    iban = fake.iban()
    accessed_at = fake.date_time_between("-1y")
    session_duration = random.randint(0, 36_000)
    download_speed = random.randint(0, 1_000)
    upload_speed = random.randint(0, 800)
    consumed_traffic = random.randint(0, 2_000_000)

    return (
        person_name, user_name, email, personal_number, birth_date,
        address, phone_number, mac_address, ip_address, iban, accessed_at,
        session_duration, download_speed, upload_speed, consumed_traffic
    )

def save_raw_data():
    logging.info(f"Started DuckDB batch processing for {date.today()}.")

    fake = _create_data("ro_RO")

    headers = [
        "person_name", "user_name", "email", "personal_number", "birth_date", "address",
        "phone", "mac_address", "ip_address", "iban", "accessed_at",
        "session_duration", "download_speed", "upload_speed", "consumed_traffic"
    ]

    if str(date.today()) == "2025-05-05":
        rows = 100_372
    else:
        rows = random.randint(0, 1_101)

    records = [_generate_record(fake) for _ in range(rows)]

    con = duckdb.connect(DUCKDB_PATH)
    con.execute("DROP TABLE IF EXISTS raw_data")

    placeholders = "(" + ",".join(["?"] * len(headers)) + ")"
    values_clause = ",".join([placeholders] * len(records))
    flattened_values = [item for record in records for item in record]

    con.execute(f"""
        CREATE TABLE raw_data AS 
        SELECT * FROM (VALUES {values_clause})
        AS t({', '.join(headers)})
    """, flattened_values)
    logging.info(f"Inserted {rows} rows into 'raw_data'.")

    con.execute("""
        CREATE OR REPLACE TABLE raw_data AS
        SELECT *, uuid() AS unique_id FROM raw_data
    """)
    logging.info("Added unique_id column.")

    yesterday_time = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    con.execute(f"""
        CREATE OR REPLACE TABLE raw_data AS
        SELECT *, TIMESTAMP '{yesterday_time}' AS updated_accessed_at
        FROM raw_data
    """)
    logging.info("Updated accessed_at to fixed timestamp.")

    con.execute("""
        COPY raw_data TO '/opt/airflow/data/raw_data.csv' (HEADER, DELIMITER ',', QUOTE '"');
    """)
    logging.info("Exported raw_data to CSV for PostgreSQL import.")

    con.close()
    logging.info(f"Finished DuckDB batch processing for {date.today()}.")
