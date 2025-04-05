import pandas as pd
import os
from dags.extract_data import base_path
from postgresql_operator import PostgresOperators
from airflow.providers.postgres.hooks.postgres import PostgresHook

def transform_dim_constructors():
    try:
        df = pd.read_csv(os.path.join(base_path, "constructors.csv"))
    except Exception as e:
        print(f"Lỗi khi đọc CSV: {e}")
        return
    columns = ["constructorId", "constructorRef", "name", "nationality"]
    dim_constructors_df = df[columns].copy()

    POSTGRES_CONN_ID = 'postgres_default'
    warehouse_operator = PostgresOperators(POSTGRES_CONN_ID)

    create_table_qr = """
        CREATE TABLE IF NOT EXISTS dim_constructors (
        constructorId INT PRIMARY KEY,
        constructorRef VARCHAR(255),
        constructorName VARCHAR(255),
        nationality VARCHAR(100)
    );
    """
    warehouse_operator.create_table(create_table_qr)

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    values = [
        (row['constructorId'], row['constructorRef'], row['name'], row['nationality'])
        for _, row in dim_constructors_df.iterrows()
    ]

    try:
        cursor.executemany("""
            INSERT INTO dim_constructors(constructorId, constructorRef, constructorName, nationality)
            VALUES (%s, %s, %s, %s)
        """, values)
        conn.commit()
        print("Đã transform và lưu dữ liệu vào dim_constructors")
    except Exception as e:
        print(f"Lỗi khi insert dữ liệu: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

    
    
