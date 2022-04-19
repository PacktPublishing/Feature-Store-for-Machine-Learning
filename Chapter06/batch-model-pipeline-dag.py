from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.papermill.operators.papermill import PapermillOperator
import uuid

dag = DAG('customer_segmentation_batch_model', description='Batch model pipeline',
          schedule_interval='@daily',
          start_date=datetime(2017, 3, 20), catchup=False)
start = DummyOperator(task_id="start", dag=dag)
run_id = str(uuid.uuid1())
feature_eng = PapermillOperator(
    task_id="feature_engineering",
    input_nb="s3://airflow-for-ml-mar-2022/notebooks/ch6_feature_engineering.ipynb",
    output_nb=f"s3://airflow-for-ml-mar-2022/notebooks/runs/ch6_feature_engineering_{run_id}.ipynb",
    dag=dag,
    trigger_rule="all_success"
)
model_prediction = PapermillOperator(
    task_id="model-prediction",
    input_nb="s3://airflow-for-ml-mar-2022/notebooks/ch6_model_prediction.ipynb",
    output_nb=f"s3://airflow-for-ml-mar-2022/notebooks/runs/ch6_model_prediction_{run_id}.ipynb",
    dag=dag,
    trigger_rule="all_success"
)

end = DummyOperator(task_id="end", dag=dag, trigger_rule="all_success")

start >> feature_eng >> model_prediction >> end
