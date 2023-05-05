import datetime

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from src.main.utils import *

# default args
default_args = {
    "owner": "D4G",
    "depends_on_past": True,
    "catchup": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "provide_context": True,
}

def choose_data_source(ti):
    first_run = ti.xcom_pull(key="first_run", task_ids="create_snapshot", dag_id='data_processing', include_prior_dates=True)
    if first_run is None:
        return "create_snapshot"
    return "update_data"



with DAG(
    dag_id="data_processing",
    start_date=datetime.datetime(2023, 4, 30),
    default_view="graph",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    _choose_data_source = BranchPythonOperator(
        task_id="choose_data_source",
        python_callable=choose_data_source,
        provide_context=True,
    )

    _create_snapshot = PythonOperator(
        python_callable=create_snapshot,
        task_id="create_snapshot",
        op_kwargs={"data_path": f"{BRONZE_LAYER_DATA_PATH}/static/raw_data.csv"},
        provide_context=True,
    )

    _update_data = PythonOperator(
        python_callable=update_data,
        task_id="update_data",
        op_kwargs={"data_path": f"{BRONZE_LAYER_DATA_PATH}/increments/2023050100.csv"},
    )

    _process_data = PythonOperator(
        python_callable=process_data,
        task_id="process_data",
        op_kwargs={"data_path": SILVER_LAYER_DTA_PATH},
        trigger_rule="one_success",
    )

    _choose_data_source >> [_create_snapshot, _update_data] >> _process_data
