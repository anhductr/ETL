import pandas as pd
import os
from postgresql_operator import PostgresOperators
from airflow.providers.postgres.hooks.postgres import PostgresHook

def transform_dim_races(**kwargs):
    ti = kwargs['ti']
    dataset_path = ti.xcom_pull(task_ids='extract.extract_and_load_to_staging')
    try:
        df = pd.read_csv(os.path.join(dataset_path, "races.csv"))
    except Exception as e:
        print(f"Lỗi khi đọc CSV: {e}")
        return

    columns = ["raceId", "name", "date", "time", "round"]
    dim_races_df = df[columns].copy()

    # Thay '\N' bằng NaN
    dim_races_df['time'] = dim_races_df['time'].replace(r'\\N', pd.NA, regex=True)

    # Chuyển chuỗi thời gian thành kiểu datetime.time, những giá trị lỗi sẽ thành NaT, sau đó .dt.time sẽ lấy phần thời gian
    dim_races_df['time'] = pd.to_datetime(dim_races_df['time'], errors='coerce').dt.time

    # Thay NaT bằng None để tránh lỗi khi insert vào PostgreSQL
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

    insert_table_querry = """
        INSERT INTO dim_races (raceId, raceName, date, timeStart, round)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (raceId) DO UPDATE SET
            raceName = EXCLUDED.raceName,
            date = EXCLUDED.date,
            timeStart = EXCLUDED.timeStart,
            round = EXCLUDED.round;
    """

    values = [
        (row['raceId'], row['name'], row['date'], row['time'], row['round'])
        for _, row in dim_races_df.iterrows()
    ]

    warehouse_operator.insert_table(insert_table_querry, values)