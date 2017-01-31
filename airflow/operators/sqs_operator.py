from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.hooks.sqs_hook import SQSHook


class SQSOperator(BaseOperator):

    @apply_defaults
    def __init__(self, queue=None, *args, **kwargs):
        super(SQSOperator, self).__init__(*args, **kwargs)
        self.queue = queue
        self.sh = SQSHook(self.queue)

    def execute(self, context):
        print self.sh.receive_messages()
