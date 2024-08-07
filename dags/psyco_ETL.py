import datetime as dt
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import opendatasets as od

psyco_1_dag = DAG("psyco_1_pipeline",schedule=None, start_date=dt.datetime.now())



def _extract_from_api():
    dataset = 'https://www.kaggle.com/datasets/arnmaud/therapist-qa'
    od.download(dataset)


def _transformation():
    pass


def _load_to_huggingface():
    pass


extract_from_api = PythonOperator(
    task_id = 'extract',
    dag=psyco_1_dag,
    python_callable=_extract_from_api,
)

transformation = PythonOperator(
    task_id = 'transform',
    dag=psyco_1_dag,
    python_callable=_transformation,
)


load_to_huggingface = PythonOperator(
    task_id = 'load',
    dag=psyco_1_dag,
    python_callable=_load_to_huggingface,
)

extract_from_api >> transformation >> load_to_huggingface