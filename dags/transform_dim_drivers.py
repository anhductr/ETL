import pandas as pd
import os
from postgresql_operator import PostgresOperators
from airflow.providers.postgres.hooks.postgres import PostgresHook


def transform_dim_drivers(**kwargs):
    ti = kwargs['ti']
    dataset_path = ti.xcom_pull(task_ids='extract.extract_and_load_to_staging')
    try:
        df = pd.read_csv(os.path.join(dataset_path, "drivers.csv"))
    except Exception as e:
        print(f"Lỗi khi đọc CSV: {e}")
        return

    columns = ["driverId", "driverRef", "number", "code", "forename", "surname", "dob", "nationality"]
    dim_drivers_df = df[columns].copy()

    # Thay '\N' bằng NaN cho toàn bộ DataFrame
    dim_drivers_df.replace(r'\\N', pd.NA, regex=True, inplace=True)

    # Chuyển NaT hoặc NaN thành None
    dim_drivers_df = dim_drivers_df.where(pd.notna(dim_drivers_df), None)

    POSTGRES_CONN_ID = 'postgres_default'
    warehouse_operator = PostgresOperators(POSTGRES_CONN_ID)

    create_table_qr = """
        CREATE TABLE IF NOT EXISTS dim_drivers (
        driverId INT PRIMARY KEY,
        driverRef VARCHAR(255),
        number INT,
        code VARCHAR(10),
        forename VARCHAR(100),
        surname VARCHAR(100),
        dob DATE,
        nationality VARCHAR(100)
    );
    """
    warehouse_operator.create_table(create_table_qr)
    
    insert_table_querry = """
        INSERT INTO dim_drivers (driverId, driverRef, number, code, forename, surname, dob, nationality)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (driverId) DO UPDATE SET
            driverRef = EXCLUDED.driverRef,
            number = EXCLUDED.number,
            code = EXCLUDED.code,
            forename = EXCLUDED.forename,
            surname = EXCLUDED.surname,
            dob = EXCLUDED.dob,
            nationality = EXCLUDED.nationality;
    """
    
    values = [
        (row['driverId'], row['driverRef'], row['number'], row['code'], row['forename'], row['surname'], row['dob'], row['nationality'])
        for _, row in dim_drivers_df.iterrows()
    ]

    warehouse_operator.insert_table(insert_table_querry, values)

    
    
