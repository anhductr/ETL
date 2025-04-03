import pandas as pd
import os
from dags.extract_data import base_path
from postgresql_operator import PostgresOperators
from airflow.providers.postgres.hooks.postgres import PostgresHook


def transform_dim_drivers():
    try:
        df = pd.read_csv(os.path.join(base_path, "drivers.csv"))
    except Exception as e:
        print(f"Lỗi khi đọc CSV: {e}")
        return

    columns = ["driverId", "driverRef", "number", "code", "forename", "surname", "dob", "nationality"]
    dim_drivers_df = df[columns]

    POSTGRES_CONN_ID = 'postgres_default'
    warehouse_operator = PostgresOperators(POSTGRES_CONN_ID)

    create_table_qr = """
        CREATE TABLE IF NOT EXISTS dim_drivers (
        driverId INT PRIMARY KEY,
        driverRef VARCHAR(255) NOT NULL,
        number INT,
        code VARCHAR(10),
        forename VARCHAR(100) NOT NULL,
        surname VARCHAR(100) NOT NULL,
        dob DATE NOT NULL,
        nationality VARCHAR(100)
    );
    """
    warehouse_operator.create_table(create_table_qr)

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    values = [
        (row['driverId'], row['driverRef'], row['number'], row['code'], row['forename'], row['surname'], row['dob'], row['nationality'])
        for _, row in dim_drivers_df.iterrows()
    ]

    try:
        cursor.executemany("""
            INSERT INTO dim_drivers (driverId, driverRef, number, code, forename, surname, dob, nationality)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, values)
        conn.commit()
        print("Đã transform và lưu dữ liệu vào dim_drivers")
    except Exception as e:
        print(f"Lỗi khi insert dữ liệu: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
    
    
    
