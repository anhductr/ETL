import pandas as pd
import os
from postgresql_operator import PostgresOperators
from airflow.providers.postgres.hooks.postgres import PostgresHook

def transform_dim_constructors(**kwargs):
    ti = kwargs['ti']
    dataset_path = ti.xcom_pull(task_ids='extract.extract_and_load_to_staging')
    try:
        df = pd.read_csv(os.path.join(dataset_path, "constructors.csv"))
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

    insert_table_querry = """
        INSERT INTO dim_constructors(constructorId, constructorRef, constructorName, nationality)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (constructorId) DO UPDATE SET
            constructorRef = EXCLUDED.constructorRef,
            constructorName = EXCLUDED.constructorName,
            nationality = EXCLUDED.nationality;
    """
    
    values = [
        (row['constructorId'], row['constructorRef'], row['name'], row['nationality'])
        for _, row in dim_constructors_df.iterrows()
    ]

    warehouse_operator.insert_table(insert_table_querry, values)

    
    
