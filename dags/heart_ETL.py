import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator
from ucimlrepo import fetch_ucirepo 
import pandas as pd
from airflow.decorators import dag, task

args = {
    "owner":"Abolfazl Andalib",
    "retries":15,

}


@dag(
        dag_id="heart_1_pipeline_v0",
        schedule=None, 
        start_date=dt.datetime.now(),
        default_args=args
)
def heart_taskflow():

    @task(multiple_outputs=True)
    def _download_from_api():
        # fetch dataset 
        heart_disease = fetch_ucirepo(id=45) 
        
        # data (as pandas dataframes) 
        X = heart_disease.data.features 
        y = heart_disease.data.targets 

        frames = [X,y]
        df = pd.concat(frames)

        return {"df":df}
        
    @task()
    def _transformation(data:pd.DataFrame):

        mean = data.mean()

        data.fillna(mean, inplace=True)

        return data.to_csv(None,sep=',')
    
    
    @task()
    def _load_to_hub(csv_transformed):
        
        print('loading...')


    data= _download_from_api()['df']
    transformed = _transformation(data=data)
    
    _load_to_hub(csv_transformed=transformed)

heart_taskflow()