from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

# Définition du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'regular_datalake_pipeline',
    default_args=default_args,
    description='Pipeline Airflow pour orchestrer les scripts de transformation',
    schedule_interval='@daily',
    catchup=False,
)

def run_script(script_name, **kwargs):
    subprocess.run(f'python /opt/airflow/src/{script_name}', shell=True, check=True)

# Opérateurs Python pour chaque transformation
unpack_task = PythonOperator(
    task_id='unpacked_to_raw',
    python_callable=run_script,
    op_args=['unpacked_to_raw.py'],
    provide_context=True,
    dag=dag,
)

# Mise à jour du nom de la tâche et du fichier script
preprocess_task = PythonOperator(
    task_id='preprocess_to_staging',
    python_callable=run_script,
    op_args=['preprocess_to_staging.py'],
    provide_context=True,
    dag=dag,
)

# Mise à jour du nom de la tâche et du fichier script
process_task = PythonOperator(
    task_id='process_to_curated',
    python_callable=run_script,
    op_args=['process_to_curated.py'],
    provide_context=True,
    dag=dag,
)

# Définition de l'ordre d'exécution
unpack_task >> preprocess_task >> process_task
