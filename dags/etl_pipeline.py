from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Definir parámetros del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definir el DAG
dag = DAG(
    'etl_telco_churn',
    default_args=default_args,
    description='ETL para analizar el churn en Telco con Airflow',
    schedule_interval=timedelta(days=1),  # Se ejecutará cada día
    catchup=False,
)

# Tarea 1: Ejecutar extract.py
extract_task = BashOperator(
    task_id='extract_data',
    bash_command='/home/jmtrujilloc/.cache/pypoetry/virtualenvs/customer-churn-etl-KwIMGp4G-py3.12/bin/python /home/jmtrujilloc/Documentos/ETL/Proyecto/customer-churn-etl/etl/extract.py',
    dag=dag,
)

# Tarea 2: Ejecutar transform.py
transform_task = BashOperator(
    task_id='transform_data',
    bash_command='/home/jmtrujilloc/.cache/pypoetry/virtualenvs/customer-churn-etl-KwIMGp4G-py3.12/bin/python /home/jmtrujilloc/Documentos/ETL/Proyecto/customer-churn-etl/etl/extract.py',
    dag=dag,
)

# Tarea 3: Ejecutar load.py
load_task = BashOperator(
    task_id='load_data',
    bash_command='/home/jmtrujilloc/.cache/pypoetry/virtualenvs/customer-churn-etl-KwIMGp4G-py3.12/bin/python /home/jmtrujilloc/Documentos/ETL/Proyecto/customer-churn-etl/etl/extract.py',
    dag=dag,
)

# Definir la secuencia de ejecución
extract_task >> transform_task >> load_task
