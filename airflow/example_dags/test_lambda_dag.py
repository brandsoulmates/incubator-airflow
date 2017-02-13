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
    'retry_delay': timedelta(minutes=1),
    'start_date':seven_days_ago,
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'wait_for_downstream': False,
    'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'trigger_rule': u'all_success'
}

user_event = {
    "payload":{},
    "type": None,
    "config_paths": {
        "api": {
            "endpoints": {
                "object_endpoint": "{id}",
                "engagements_endpoint": "{id}/{engagement_type}",
                "recent_posts_endpoint": "{id}/posts",
                "private_posts_endpoint": "{id}/promotable_posts",
                "insights_endpoint": "{id}/insights"
            },
            "api_config":{
                "api_url":"https://graph.facebook.com/{api_ver}/",
                "network_name":"facebook",
                "policy_engine":"single_token",
                "policy": {
                    "network_name": "facebook",
                    "policy": {
                        "max": 200,
                        "window": 15,
                        "min": 10
                        },
                    "key": "ayz-cars/fb-quiz-tokens/86753a09e"
                    },
                "producer_type":"s3"
            }
            
        },
        "producer": {
            "bucket":"analytics-raw-json"
        }
    },
    "call_priority": "analytics"
}

user_event["payload"] = {"max_pages": 2,
               "channel_ids":['10108478465469099'],
               "get_channel_posts":1}
user_event["type"] = "user"

dag = DAG('lambda_test', default_args=default_args,
          schedule_interval="@once")

# t2 depends on t1
t1 = AwsLambdaOperator(
    task_id='get_user_and_posts',                   
    event_json=user_event,
    function_name='jmolle-testfunction',
    aws_lambda_conn_id = 'aws_default',
    version=None,
    invocation_type = 'RequestResponse',
    dag=dag)


t2 = AwsLambdaOperator(
    task_id='get_post_engagements',
    event_json=user_event,
    function_name='jmolle-testfunction',
    aws_lambda_conn_id = 'aws_default',
    version=None,
    invocation_type = 'RequestResponse',
    dag=dag)

t2.set_upstream(t1)
