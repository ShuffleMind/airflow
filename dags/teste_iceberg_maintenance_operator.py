from operators.trino_iceberg_maintenance_operator import TrinoIcebergMaintenanceOperator
from datetime import *
import pendulum

from airflow.decorators import task
from airflow.models import DAG


local_tz = pendulum.timezone("America/Sao_Paulo")

default_args = {
    'max_active_runs': 2,
    'owner': 'data-platform',
    'start_date': datetime(2022, 1, 24, tzinfo=local_tz)
}

with DAG(dag_id="test_maintenance_operator", default_args=default_args, start_date=datetime(2022, 3, 4), schedule_interval=None) as dag:
    hello_task = TrinoIcebergMaintenanceOperator(
        task_id="sample-task",
        table_name="iceberg.ness_psp_receivable_compliance.contract_receivable_unit_reached_flat",
        conn_id="trino_admin"
    )