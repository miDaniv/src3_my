from airflow import DAG
from datetime import datetime

from dag_tasks import get_tasks

# Define the default arguments for DAG.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}
# Define the DAG.
dag = DAG(
    'extract_raw_data_pipeline',
    default_args=default_args,
    description='DataDriven Main Pipeline.',
    schedule_interval="* 7 * * *",
    start_date=datetime(2024, 9, 22),
    catchup=False,
)

(
    extract_raw_data_task,
    create_raw_schema_task,
    create_raw_table_task,
    load_raw_data_task,
    run_dbt_staging_task,
    run_dbt_trusted_task
) = get_tasks(dag)
# Set the task in the DAG
[extract_raw_data_task, create_raw_schema_task] >> create_raw_table_task
create_raw_table_task >> load_raw_data_task >> run_dbt_staging_task
run_dbt_staging_task >> run_dbt_trusted_task
