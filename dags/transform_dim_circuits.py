import pandas as pd
import os
from postgresql_operator import PostgresOperators
from airflow.providers.postgres.hooks.postgres import PostgresHook

def transform_dim_circuits(**kwargs):
    ti = kwargs['ti']
    dataset_path = ti.xcom_pull(task_ids='extract.extract_and_load_to_staging')
    try:
        df = pd.read_csv(os.path.join(dataset_path, "circuits.csv"))
    except Exception as e:
        print(f"Lỗi khi đọc CSV: {e}")
        return

    columns = ["circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt"]
    dim_circuits_df = df[columns].copy()

    POSTGRES_CONN_ID = 'postgres_default'
    warehouse_operator = PostgresOperators(POSTGRES_CONN_ID)

    create_table_qr = """
    CREATE TABLE IF NOT EXISTS dim_circuits (
        circuitId INT PRIMARY KEY,
        circuitRef VARCHAR(100),
        circuitName VARCHAR(255),
        location VARCHAR(255),
        country VARCHAR(100),
        lat FLOAT,
        lng FLOAT,
        alt INT
    );
    """
    warehouse_operator.create_table(create_table_qr)
    
    insert_table_querry = """
        INSERT INTO dim_circuits(circuitId, circuitRef, circuitName, location, country, lat, lng, alt)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (circuitId) DO UPDATE SET
            circuitRef = EXCLUDED.circuitRef,
            circuitName = EXCLUDED.circuitName,
            location = EXCLUDED.location,
            country = EXCLUDED.country,
            lat = EXCLUDED.lat,
            lng = EXCLUDED.lng,
            alt = EXCLUDED.alt;
    """

    values = [
        (row['circuitId'], row['circuitRef'], row['name'], row['location'], row['country'], row['lat'], row['lng'], row['alt'])
        for _, row in dim_circuits_df.iterrows()
    ]

    warehouse_operator.insert_table(insert_table_querry, values)
