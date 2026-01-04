
# DAG THAT AUTOMATICALLY RUN THE ETL PIPELINE ON EVERY SEVEN DAYS SCHEDULE



from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
from scripts.project_automate_etl import main


# Default arguments for DAG
default_args = {
    'owner': 'Teddy',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}




# DAG
dag = DAG(
    'Project_etl_job',
    default_args=default_args,
    description='Run Python batch insert every 7 days',
    schedule_interval='@weekly',  # runs per week
    start_date=datetime(2025, 12, 22),
    catchup=False)



# PythonOperator to run main function of the etl pipeline
run_project_etl = PythonOperator(
    task_id='project_etl_script',
    python_callable=main,
    dag=dag)

run_project_etl
