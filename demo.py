import math
import random

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owners': 'Aman'
}

with DAG(
        dag_id='candidate_recruiter',
        schedule_interval=None,
        start_date=days_ago(2)
) as dag:
    def random_number_between_1_and_20() -> int:
        return random.randint(1, 20)


    def is_prime(num: int) -> bool:
        if num % 2 == 0 and num > 2:
            return False
        return all(num % i for i in range(3, int(math.sqrt(num)) + 1, 2))


    def is_candidate_a_mamasnik(ti: TaskInstance) -> bool:
        candidate_random_number = int(ti.xcom_pull(task_ids="select_a_random_number"))
        if candidate_random_number == 6 or is_prime(candidate_random_number):
            return True
        else:
            return False


    def print_candidate_is_a_mamasnik(ti: TaskInstance) -> None:
        candidate_random_number = int(ti.xcom_pull(task_ids="select_a_random_number"))
        print(f'The candidate is a Mamasnik, the random number is {candidate_random_number}')


    def print_candidate_is_a_matmonist(ti: TaskInstance) -> None:
        candidate_random_number = int(ti.xcom_pull(task_ids="select_a_random_number"))
        print(f'The candidate is a Matmonist, the random number is {candidate_random_number}.'
              f'He will be a Viking forever!')


    select_a_random_number = PythonOperator(task_id='select_a_random_number',
                                            python_callable=random_number_between_1_and_20,
                                            do_xcom_push=True)

    check_if_candidate_is_a_mamasnik = BranchPythonOperator(task_id='check_if_candidate_is_a_mamasnik',
                                                            python_callable=is_candidate_a_mamasnik)

    candidate_a_mamasnik = PythonOperator(task_id='candidate_a_mamasnik',
                                          python_callable=print_candidate_is_a_mamasnik)

    candidate_a_matmonist = PythonOperator(task_id='candidate_a_matmonist',
                                           python_callable=print_candidate_is_a_matmonist)

    select_a_random_number >> check_if_candidate_is_a_mamasnik >> [candidate_a_mamasnik, candidate_a_matmonist]
