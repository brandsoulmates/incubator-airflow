import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.bash_operator import BaseOperator
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class RedshiftToSQS(BaseOperator):

    def __init__(self, table,
                 s3_bucket,
                 s3_key,
                 autocommit,
                 *args, **kwargs):
        super(RedshiftToSQS, self).__init__(*args, **kwargs)
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.autocommit = autocommit

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.s3 = S3Hook(s3_conn_id=self.s3_conn_id)
        a_key, s_key = self.s3.get_credentials()
