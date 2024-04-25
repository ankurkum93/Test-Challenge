from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
        'owner' : 'airflow',
        'start_date' : datetime(2022, 11, 12),
}

with DAG(dag_id='Creating_database',
        default_args=default_args,
        schedule_interval='@once',
        catchup=False
) as dag:
    start = DummyOperator(task_id = 'start', dag = dag)

    Database_creation = BashOperator(
        task_id='CreatingDatabase',
        bash_command = f'python3 /opt/airflow/dags/Scripts/Database_creation.py',
        dag=dag
        )
    Testing_database_creation = BashOperator(
        task_id='Testing_database_creation',
        bash_command = f'python3 /opt/airflow/dags/Scripts/Testing_database_creation.py',
        dag=dag
        )
        
    Copying_Tables = BashOperator(
        task_id='Copying_Tables',
        bash_command = f'python3 /opt/airflow/dags/Scripts/Copying_tables.py',
        dag=dag
        )
    tests = BashOperator(
        task_id='Testing_Database',
        bash_command = f'python3 /opt/airflow/dags/Scripts/tests.py',
        dag=dag
        )
    Queries = BashOperator(
        task_id='Query',
        bash_command = f'python3 /opt/airflow/dags/Scripts/Queries.py',
        dag=dag
        )
    end = DummyOperator(task_id = 'end', dag = dag)

    start >> Database_creation>> Testing_database_creation >> Copying_Tables >> tests >> Queries >> end