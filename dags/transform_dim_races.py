import pandas as pd
import os
from dags.extract_data import base_path
from postgresql_operator import PostgresOperators
from airflow.providers.postgres.hooks.postgres import PostgresHook

def transform_dim_races():
    try:
        df = pd.read_csv(os.path.join(base_path, "races.csv"))
    except Exception as e:
        print(f"Lỗi khi đọc CSV: {e}")
        return

    columns = ["raceId", "name", "date", "time", "round"]
    dim_races_df = df[columns].copy()

    # Thay '\N' bằng NaN
    dim_races_df['time'] = dim_races_df['time'].replace(r'\\N', pd.NA, regex=True)

    # Ép kiểu cột 'time' thành datetime.time, bỏ qua lỗi
    dim_races_df['time'] = pd.to_datetime(dim_races_df['time'], errors='coerce').dt.time

    # Thay NaT (Not a Time) bằng None để tránh lỗi khi insert vào PostgreSQL
    dim_races_df['time'] = dim_races_df['time'].where(pd.notna(dim_races_df['time']), None)

    POSTGRES_CONN_ID = 'postgres_default'
    warehouse_operator = PostgresOperators(POSTGRES_CONN_ID)

    create_table_qr = """
        CREATE TABLE IF NOT EXISTS dim_races (
        raceId INT PRIMARY KEY,
        raceName VARCHAR(255),
        date DATE,
        timeStart TIME NULL,
        round INT
    );
    """
    warehouse_operator.create_table(create_table_qr)

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    values = [
        (row['raceId'], row['name'], row['date'], row['time'], row['round'])
        for _, row in dim_races_df.iterrows()
    ]

    try:
        cursor.executemany("""
            INSERT INTO dim_races (raceId, raceName, date, timeStart, round)
            VALUES (%s, %s, %s, %s, %s)
        """, values)
        conn.commit()
        print("Đã transform và lưu dữ liệu vào dim_races")
    except Exception as e:
        print(f"Lỗi khi insert dữ liệu: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
