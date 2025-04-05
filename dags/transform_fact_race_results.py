import pandas as pd
import os
from dags.extract_data import base_path
from postgresql_operator import PostgresOperators
from airflow.providers.postgres.hooks.postgres import PostgresHook

def transform_fact_race_results():
    try:
        df_races = pd.read_csv(os.path.join(base_path, "races.csv"))
        df_results = pd.read_csv(os.path.join(base_path, "results.csv"))
    except Exception as e:
        print(f"Lỗi khi đọc CSV: {e}")
        return
    
    # Kết hợp dữ liệu
    df = pd.merge(df_results, df_races, on='raceId', how='left')
    # Kiểm tra các cột trong DataFrame
    print("Các cột trong DataFrame sau khi merge:")
    print(df.columns)

    columns = ["raceId", "circuitId", "year", "resultId", "position", "points", "fastestLapTime", "statusId", "constructorId", "driverId"]
    fact_race_results__df = df[columns].copy()

    POSTGRES_CONN_ID = 'postgres_default'
    warehouse_operator = PostgresOperators(POSTGRES_CONN_ID)

    # Thay '\N' bằng NaN cho toàn bộ DataFrame
    fact_race_results__df = fact_race_results__df.replace(r'\\N', pd.NA, regex=True)

    # Duyệt từng cột và cố gắng ép kiểu datetime, sau đó lấy .dt.time nếu muốn
    for col in fact_race_results__df.columns:
        fact_race_results__df[col] = fact_race_results__df[col].where(pd.notna(fact_race_results__df[col]), None)

    create_table_qr = """
        CREATE TABLE IF NOT EXISTS fact_race_results (
        race_result_Id INT PRIMARY KEY,
        position TEXT,              
        points FLOAT,               
        fastestLapTime TEXT,
        circuitId INT,
        seasonId INT,        
        constructorId INT,
        driverId INT,
        raceId INT,
        statusId INT, 
        FOREIGN KEY (circuitId) REFERENCES dim_circuits (circuitId),
        FOREIGN KEY (seasonId) REFERENCES dim_seasons (seasonId),
        FOREIGN KEY (constructorId) REFERENCES dim_constructors (constructorId),
        FOREIGN KEY (driverId) REFERENCES dim_drivers (driverId),
        FOREIGN KEY (raceId) REFERENCES dim_races (raceId),
        FOREIGN KEY (statusId) REFERENCES dim_status (statusId)
    );
    """
    warehouse_operator.create_table(create_table_qr)

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    values = [
        (row['resultId'], row['position'], row['points'], row['fastestLapTime'], row['circuitId'], row['year'], row['constructorId'], row['driverId'], row['raceId'], row['statusId'])
        for _, row in fact_race_results__df.iterrows()
    ]

    try:
        cursor.executemany("""
            INSERT INTO fact_race_results (race_result_Id, position, points, fastestLapTime, circuitId, seasonId, constructorId, driverId, raceId, statusId)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, values)
        conn.commit()
        print("Đã transform và lưu dữ liệu vào fact_race_results")
    except Exception as e:
        print(f"Lỗi khi insert dữ liệu: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
    
    