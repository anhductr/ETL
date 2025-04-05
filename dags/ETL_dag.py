from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from dags.transform_dim_races import transform_dim_races
from dags.transform_dim_circuits import transform_dim_circuits
from dags.transform_dim_seasons import transform_dim_seasons
from dags.transform_dim_status import transform_dim_status
from dags.transform_dim_drivers import transform_dim_drivers
from dags.transform_dim_constructors import transform_dim_constructors
from dags.transform_fact_race_results import transform_fact_race_results
from extract_data import extract_and_load_to_staging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 29),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'e_commerce_dw_etl',
    default_args=default_args,
    description='ETL process for E-commerce Data Warehouse',
    schedule_interval=timedelta(days=1),
) as dag:

    # Task Extract
    with TaskGroup("extract") as extract_group:
        extract_task = PythonOperator(
            task_id='extract_and_load_to_staging',
            python_callable=extract_and_load_to_staging,
            provide_context=True,
        )

    # Task Group Transform
    with TaskGroup("transform") as transform_group: 
        task_dim_circuits = PythonOperator(
            task_id='transform_dim_circuits',
            python_callable=transform_dim_circuits,
        )
        
        task_dim_races = PythonOperator( 
            task_id='transform_dim_races',
            python_callable=transform_dim_races,
        )
        
        task_dim_status = PythonOperator( 
            task_id='transform_dim_status',
            python_callable=transform_dim_status,
        )
        
        task_dim_drivers = PythonOperator(
            task_id='transform_dim_drivers',
            python_callable=transform_dim_drivers,
        )
        
        task_dim_constructors = PythonOperator(
            task_id='transform_dim_constructors',
            python_callable=transform_dim_constructors,
        )

        task_dim_seasons = PythonOperator(
            task_id='transform_dim_seasons',
            python_callable=transform_dim_seasons,
        )

    # Task Group Load
    with TaskGroup("load") as load_group:
        task_fact_orders = PythonOperator(
            task_id='transform_fact_race_results',
            python_callable=transform_fact_race_results,
        )

    #Thiết lập dependencies
    extract_group >> transform_group >> load_group


