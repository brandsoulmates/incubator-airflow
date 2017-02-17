
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.hooks.sqs_hook import SQSHook


class SQSOperator(BaseOperator):

    @apply_defaults
    def __init__(self, queue=None,
                 delete_on_recieve=False,
                 xcom_push=False,
                 provide_context=False,
                 *args, **kwargs):
        super(SQSOperator, self).__init__(*args, **kwargs)
        self.xcom_push_flag = xcom_push
        self.queue = queue
        self.delete_on_recieve = delete_on_recieve

    def execute(self, context):
        self.sh = SQSHook(self.queue)
        val_complete = self.sh.receive_messages()
        val = val_complete.get_body()
        if self.delete_on_recieve:
            self.sh.delete_messages(val_complete)
        if self.xcom_push_flag:
            return val
        else:
            print val
