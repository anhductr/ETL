import pandas as pd
import os
from dags.extract_data import base_path
from postgresql_operator import PostgresOperators
from airflow.providers.postgres.hooks.postgres import PostgresHook

def transform_dim_circuits():
    print(base_path)
    try:
        df = pd.read_csv(os.path.join(base_path, "circuits.csv"))
        print("đã đọc được circuits")
    except Exception as e:
        print(f"Lỗi khi đọc CSV: {e}")
        return

    columns = ["circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt"]
    dim_circuits_df = df[columns].copy()

    POSTGRES_CONN_ID = 'postgres_default'
    warehouse_operator = PostgresOperators(POSTGRES_CONN_ID)
    print("tạo dc postgres")

    create_table_qr = """
    CREATE TABLE IF NOT EXISTS dim_circuits (
        circuitId INT PRIMARY KEY,
        circuitRef VARCHAR(100),
        circuitName VARCHAR(255),
        location VARCHAR(255),
        country VARCHAR(100),
        lat FLOAT,
        lng FLOAT,
        alt INT
    );
    """
    warehouse_operator.create_table(create_table_qr)

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    values = [
        (row['circuitId'], row['circuitRef'], row['name'], row['location'], row['country'], row['lat'], row['lng'], row['alt'])
        for _, row in dim_circuits_df.iterrows()
    ]

    try:
        cursor.executemany("""
        INSERT INTO dim_circuits(circuitId, circuitRef, circuitName, location, country, lat, lng, alt)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, values)
        conn.commit()
        print("Đã transform và lưu dữ liệu vào dim_circuits")
    except Exception as e:
        print(f"Lỗi khi insert dữ liệu: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
