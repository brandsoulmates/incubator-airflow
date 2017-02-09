import logging

from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException


class ViewOperator(BaseOperator):

    @apply_defaults
    def __init__(self, sql,
                 redshift_conn_id="redshift_default",
                 autocommit=True,
                 * args, **kwargs):
        super(ViewOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.redshift_conn_id = redshift_conn_id
        self.autocommit = autocommit

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        logging.info("Start the execution")
        try:
            logging.info("SQL: %s" % self.sql)
            redshift.run(self.sql, self.autocommit)
            logging.info("End of the execution")
        except:
            logging.error("Execution failed!")
            raise AirflowException
