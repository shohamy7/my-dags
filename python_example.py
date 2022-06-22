"""Example DAG demonstrating the usage of the PythonOperator."""
import time
from datetime import datetime
from pprint import pprint

from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator

default_args = {
    'owner': 'shoham'
}

with DAG(
    dag_id='example_python_operator',
    schedule_interval=None,
    start_date=datetime(2021, 9, 12, 13, 2, 0),
    end_date=datetime(2021, 9, 12, 13, 5, 0),
    default_args=default_args,
    catchup=False,
    tags=['example'],
) as dag:
    # [START howto_operator_python]
    def print_context(ds, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        pprint(kwargs)
        import socket
        print(socket.gethostname())
        from time import sleep
        sleep(15)
        print(ds)
        return 'Whatever you return gets printed in the logs'

    run_this = PythonOperator(
        task_id='print_the_context',
        python_callable=print_context,
        queue='kubernetes'
    )
    # [END howto_operator_python]
