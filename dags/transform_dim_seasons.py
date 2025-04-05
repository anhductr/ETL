import pandas as pd
import numpy as np
import os
from dags.extract_data import base_path
from postgresql_operator import PostgresOperators
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extensions import register_adapter, AsIs

def transform_dim_seasons():
    # Đăng ký adapter cho numpy.int64
    register_adapter(np.int64, AsIs)
    
    try:
        df = pd.read_csv(os.path.join(base_path, "seasons.csv"))
    except Exception as e:
        print(f"Lỗi khi đọc CSV: {e}")
        return

    columns = ["year"]
    dim_seasons_df = df[columns].copy()

    POSTGRES_CONN_ID = 'postgres_default'
    warehouse_operator = PostgresOperators(POSTGRES_CONN_ID)

    create_table_qr = """
        CREATE TABLE IF NOT EXISTS dim_seasons (
        seasonId INT PRIMARY KEY
    );
    """
    warehouse_operator.create_table(create_table_qr)

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Chuyển đổi giá trị trước khi insert để đảm bảo không có numpy.int64
    values = [
        (row['year'],)  # Ép kiểu int để PostgreSQL chấp nhận
        for _, row in dim_seasons_df.iterrows()
    ]

    try:
        cursor.executemany("""
            INSERT INTO dim_seasons (seasonId)
            VALUES (%s)
        """, values)
        conn.commit()
        print("Đã transform và lưu dữ liệu vào dim_seasons")
    except Exception as e:
        print(f"Lỗi khi insert dữ liệu: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
