import pandas as pd
import numpy as np
import os
from postgresql_operator import PostgresOperators
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extensions import register_adapter, AsIs

def transform_dim_seasons(**kwargs):
    ti = kwargs['ti']
    dataset_path = ti.xcom_pull(task_ids='extract.extract_and_load_to_staging')
    
    # Đăng ký adapter cho numpy.int64
    register_adapter(np.int64, AsIs)
    
    try:
        df = pd.read_csv(os.path.join(dataset_path, "seasons.csv"))
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

    insert_table_querry = """
        INSERT INTO dim_seasons (seasonId)
        VALUES (%s)
        ON CONFLICT (seasonId) DO NOTHING
    """

    values = [
        (row['year'],) 
        for _, row in dim_seasons_df.iterrows()
    ]

    warehouse_operator.insert_table(insert_table_querry, values)
