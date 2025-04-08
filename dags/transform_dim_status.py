import pandas as pd
import os
from postgresql_operator import PostgresOperators
from airflow.providers.postgres.hooks.postgres import PostgresHook

def transform_dim_status(**kwargs):
    ti = kwargs['ti']
    dataset_path = ti.xcom_pull(task_ids='extract.extract_and_load_to_staging')
    
    try:
        df = pd.read_csv(os.path.join(dataset_path, "status.csv"))
    except Exception as e:
        print(f"Lỗi khi đọc CSV: {e}")
        return

    columns = ["statusId", "status"]
    dim_status_df = df[columns].copy()

    POSTGRES_CONN_ID = 'postgres_default'
    warehouse_operator = PostgresOperators(POSTGRES_CONN_ID)

    create_table_qr = """
        CREATE TABLE IF NOT EXISTS dim_status (
        statusId INT PRIMARY KEY,
        status VARCHAR(255)
    );
    """
    warehouse_operator.create_table(create_table_qr)

    insert_table_querry = """
        INSERT INTO dim_status (statusId, status)
        VALUES (%s, %s)
        ON CONFLICT (statusId) DO UPDATE SET
            status = EXCLUDED.status
    """

    values = [
        (row['statusId'], row['status'])
        for _, row in dim_status_df.iterrows()
    ]

    warehouse_operator.insert_table(insert_table_querry, values)
