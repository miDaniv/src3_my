from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from data_generate import save_raw_data

def get_tasks(dag):
    extract_raw_data_task = PythonOperator(
        task_id='extract_raw_data',
        python_callable=save_raw_data,
        dag=dag,
    )
    create_raw_schema_task = SQLExecuteQueryOperator(
        task_id='create_raw_schema',
        conn_id='postgres_conn',
        sql='CREATE SCHEMA IF NOT EXISTS driven_raw;',
        dag=dag,
    )
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
    check_updated_accessed_at_column = SQLExecuteQueryOperator(
        task_id='check_updated_accessed_at_column',
        conn_id='postgres_conn',
        sql="""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_schema = 'driven_raw'
                  AND table_name = 'raw_batch_data'
                  AND column_name = 'updated_accessed_at'
            ) THEN
                ALTER TABLE driven_raw.raw_batch_data
                ADD COLUMN updated_accessed_at TIMESTAMP;
            END IF;
        END
        $$;
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
            session_duration, download_speed, upload_speed, consumed_traffic, unique_id, updated_accessed_at
        ) 
        FROM '/opt/airflow/data/raw_data.csv' 
        DELIMITER ',' 
        CSV HEADER 
        QUOTE '"';
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
        check_updated_accessed_at_column,
        load_raw_data_task,
        run_dbt_staging_task,
        run_dbt_trusted_task
    )
