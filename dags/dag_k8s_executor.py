from datetime import datetime, timedelta
from airflow import DAG
from kubernetes import client as k8s

from airflow.operators.python import PythonOperator

# Define default_args dictionary to specify the default parameters of the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def print_greetings(try_number: int, **kwargs):
    import os
    greetings = os.getenv('GREETING', "No greeting")
    if try_number > 0:
        print(f"Greeting: {greetings}")
    else:
        raise Exception("Error!")


# Define the DAG object with the specified parameters
dag = DAG(
    'example_k8s_executor_dag',
    default_args=default_args,
    schedule_interval=None,  # This DAG is triggered manually
)

# Define the BashOperator task that prints the greeting to stdout
print_greeting_task = PythonOperator(
    task_id='print_greeting',
    python_callable=print_greetings,
    executor_config={
        "pod_override": k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(name="base", namespace="hello"),
            spec=k8s.V1PodSpec(
                containers=k8s.V1Container(
                    name="base",
                    env=k8s.V1EnvVar(
                        name="GREETING",
                        value="Hello World!"
                    )
                )
            )
        )
    },
    op_kwargs={"try_number": "{{ task_instance.try_number }}"},
    dag=dag,
)

# The DAG should be manually triggered by the user when needed.
# You can trigger it using the Airflow CLI or the Airflow UI.
# Example CLI command: `airflow trigger_dag example_k8s_executor_dag`
