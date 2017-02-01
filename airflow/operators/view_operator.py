from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.hooks.reshift_hook import RedshiftHook


class ViewOperator(BaseOperator):

    @apply_defaults
    def __init__(self, sql,
                 postgres_conn_id="",
                 parameters=None,
                 autocommit=True,
                 * args, **kwargs):
        super(ViewOperator, self).__init__(*args, **kwargs)
        self.sql = sql

    def execute(self, context):
        print self.sh.receive_messages()

    def initialize_connetion(self, conn_args):
        print "hello!"
