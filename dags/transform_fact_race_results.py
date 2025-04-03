import pandas as pd
from postgresql_operator import PostgresOperators

def transform_fact_race_results(POSTGRES_CONN_ID):
    staging_operator = PostgresOperators('postgres', POSTGRES_CONN_ID)
    warehouse_operator = PostgresOperators('postgres')
    
    # Đọc dữ liệu từ staging
    #thieu seasonID, TimeID, circuitId
    df_races = staging_operator.get_data_to_pd("SELECT raceId, circuitId, year as seasonId as seasonId FROM staging.stg_races")
    df_results = staging_operator.get_data_to_pd("SELECT resultId as race_results_id, position, points, fastestLapTime, statusId, time, constructorId, raceId, driverId FROM staging.stg_results") 

    # Kết hợp dữ liệu
    df = pd.merge(df_results, df_races, on='raceId', how='left')

    # Kiểm tra các cột trong DataFrame
    print("Các cột trong DataFrame sau khi merge:")
    print(df.columns)

    # Tạo các foreign keys
    df['seasonId_key'] = df['seasonId']
    df['statusId_key'] = df['statusId']
    df['constructorId_key'] = df['constructorId']
    df['circuitId_key'] = df['circuitId']
    df['raceId_key'] = df['raceId']
    df['driverId_key'] = df['driverId']

    # Lưu dữ liệu vào bảng fact_race_results
    warehouse_operator.save_data_to_postgres(
        df,
        'fact_race_results',
        schema='warehouse',
        if_exists='replace'
    )
    
    print("Đã transform và lưu dữ liệu vào fact_race_results")