import pandas as pd
import os
from dags.extract_data import base_path
from postgresql_operator import PostgresOperators
from airflow.providers.postgres.hooks.postgres import PostgresHook

def transform_dim_status():
    try:
        df = pd.read_csv(os.path.join(base_path, "status.csv"))
    except Exception as e:
        print(f"Lỗi khi đọc CSV: {e}")
        return

    columns = ["statusId", "status"]
    dim_status_df = df[columns]

    POSTGRES_CONN_ID = 'postgres_default'
    warehouse_operator = PostgresOperators(POSTGRES_CONN_ID)

    create_table_qr = """
        CREATE TABLE IF NOT EXISTS dim_status (
        statusId INT PRIMARY KEY,
        status VARCHAR(255)
    );
    """
    warehouse_operator.create_table(create_table_qr)

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    values = [
        (row['statusId'], row['status'])
        for _, row in dim_status_df.iterrows()
    ]

    try:
        cursor.executemany("""
            INSERT INTO dim_status (seasonId)
            VALUES (%s)
        """, values)
        conn.commit()
        print("Đã transform và lưu dữ liệu vào dim_status")
    except Exception as e:
        print(f"Lỗi khi insert dữ liệu: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
