"""
Code that goes along with the Airflow tutorial located at:
https://github.com/airbnb/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.operators.aws_lambda_operator import AwsLambdaOperator
from datetime import datetime, timedelta

seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                  datetime.min.time())

default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date':seven_days_ago,
}

dag = DAG('lambda_test', default_args=default_args)

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = AwsLambdaOperator(
    task_id='fail_a_lambda_1',                   
    event_json={'my_param': 'Parameter I passed in'},
    function_name='function_name',
    aws_lambda_conn_id = 'aws_default',
    version=None,
    invocation_type = 'RequestResponse',
    dag=dag)

t2 = AwsLambdaOperator(
    task_id='fail_a_lambda_2',
    event_json={'my_param': 'Parameter I passed in'},
    function_name='function_name',
    aws_lambda_conn_id = 'aws_default',
    version=None,
    invocation_type = 'RequestResponse',
    retries=3,
    dag=dag)


t3 = AwsLambdaOperator(
    task_id='fail_a_lambda_3',
    event_json={'my_param': 'Parameter I passed in'},
    function_name='function_name',
    aws_lambda_conn_id = 'aws_default',
    version=None,
    invocation_type = 'RequestResponse',
    dag=dag)

t2.set_upstream(t1)
t3.set_upstream(t1)