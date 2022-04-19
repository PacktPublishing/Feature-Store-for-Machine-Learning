from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.papermill.operators.papermill import PapermillOperator

# apache-airflow-providers-papermill
# docker build --rm --build-arg AIRFLOW_VERSION="2.2.5" -t puckel/docker-airflow .
dag = DAG('customer_segmentation_online_model', description='Online model pipeline',
          schedule_interval='@daily',
          start_date=datetime(2017, 3, 20), catchup=False)
start = DummyOperator(task_id="start")
run_time = datetime.now()
feature_eng = PapermillOperator(
    task_id="feature_engineering",
    input_nb="s3://airflow-for-ml-mar-2022/notebooks/ch6_feature_engineering.ipynb",
    output_nb=f"s3://airflow-for-ml-mar-2022/notebooks/runs/ch6_feature_engineering_{run_time}.ipynb",
    trigger_rule="all_success",
    dag=dag
)
sync_offline_to_online = PapermillOperator(
    task_id="sync_offline_to_online",
    input_nb="s3://airflow-for-ml-mar-2022/notebooks/ch6_sync_offline_online.ipynb",
    output_nb=f"s3://airflow-for-ml-mar-2022/notebooks/runs/ch6_sync_offline_online_{run_time}.ipynb",
    trigger_rule="all_success",
    dag=dag
)

end = DummyOperator(task_id="end", trigger_rule="all_success")

start >> feature_eng >> sync_offline_to_online >> end
