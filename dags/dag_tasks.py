from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from data_generate import save_raw_data

def get_tasks(dag):
    # Define extract raw data task.
    extract_raw_data_task = PythonOperator(
        task_id='extract_raw_data',
        python_callable=save_raw_data,
        dag=dag,
    )
    # Define create raw schema task.
    create_raw_schema_task = SQLExecuteQueryOperator(
        task_id='create_raw_schema',
        conn_id='postgres_conn',
        sql='CREATE SCHEMA IF NOT EXISTS driven_raw;',
        dag=dag,
    )
    # Define create raw table task.
    create_raw_table_task = SQLExecuteQueryOperator(
        task_id='create_raw_table',
        conn_id='postgres_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS driven_raw.raw_batch_data (
                person_name VARCHAR(100),
                user_name VARCHAR(100),
                email VARCHAR(100),
                personal_number NUMERIC, 
                birth_date VARCHAR(100), 
                address VARCHAR(100),
                phone VARCHAR(100), 
                mac_address VARCHAR(100),
                ip_address VARCHAR(100),
                iban VARCHAR(100),
                accessed_at TIMESTAMP,
                session_duration INT,
                download_speed INT,
                upload_speed INT,
                consumed_traffic INT,
                unique_id VARCHAR(100)
            );
        """,
        dag=dag
    )
    # Define load CSV data into the table task.
    load_raw_data_task = SQLExecuteQueryOperator(
        task_id='load_raw_data',
        conn_id='postgres_conn',
        sql="""
        COPY driven_raw.raw_batch_data(
            person_name, user_name, email, personal_number, birth_date,
            address, phone, mac_address, ip_address, iban, accessed_at,
            session_duration, download_speed, upload_speed, consumed_traffic, unique_id
        ) 
        FROM '/opt/airflow/data/raw_data.csv' 
        DELIMITER ',' 
        CSV HEADER;
        """,
        dag=dag
    )
    # Define staging dbt models run task.
    run_dbt_staging_task = BashOperator(
        task_id='run_dbt_staging',
        bash_command='set -x; cd /opt/airflow/dbt && dbt run --select tag:staging',
        dag=dag,
    )

    run_dbt_trusted_task = BashOperator(
        task_id='run_dbt_trusted',
        bash_command='set -x; cd /opt/airflow/dbt && dbt run --select tag:trusted',
        dag=dag,
    )
    # Define trusted dbt models run task.
    return (
        extract_raw_data_task,
        create_raw_schema_task,
        create_raw_table_task,
        load_raw_data_task,
        run_dbt_staging_task,
        run_dbt_trusted_task
    )
