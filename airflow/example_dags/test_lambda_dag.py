"""
Code that goes along with the Airflow tutorial located at:
https://github.com/airbnb/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.operators.aws_lambda_operator import AwsLambdaOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('lambda_test', default_args=default_args)

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = AwsLambdaOperator(
    event_json={'my_param': 'Parameter I passed in'},
    function_name,
    aws_lambda_conn_id = 'aws_lambda_default',
    version=None,
    invocation_type = 'RequestResponse',
    dag=dag)

t2 = AwsLambdaOperator(
    event_json={'my_param': 'Parameter I passed in'},
    function_name='function_name',
    aws_lambda_conn_id = 'aws_lambda_default',
    version=None,
    invocation_type = 'RequestResponse',
    retries=3,
    dag=dag)

templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
"""

t3 = AwsLambdaOperator(
    event_json={'my_param': 'Parameter I passed in'},
    function_name,
    aws_lambda_conn_id = 'aws_lambda_default',
    version=None,
    invocation_type = 'RequestResponse',
    dag=dag)

t2.set_upstream(t1)
t3.set_upstream(t1)