import logging
import airflow
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.hooks.sqs_hook import SQSHook


class SQSOperator(BaseOperator):

    @apply_defaults
    def __init__(self, queue=None,
                 max_num=10,
                 delete_on_recieve=False,
                 xcom_push=False,
                 provide_context=False,
                 *args, **kwargs):

        super(SQSOperator, self).__init__(*args, **kwargs)
        self.max_num = max_num
        self.xcom_push_flag = xcom_push
        self.queue = queue
        self.delete_on_recieve = delete_on_recieve
        self.kwargs = kwargs

    def execute(self, context):
        self.sh = SQSHook(self.queue)
        val_complete = self.sh.receive_bulk(self.max_num, self.delete_on_recieve)
        new_vals = []
        for x in range(0, len(val_complete)):
            new_vals.append(val_complete[x].body)
            logging.info(val_complete[x].body)
            if self.xcom_push_flag:
                context['ti'].xcom_push(key=str(x), value=val_complete[x].body)
            logging.info(val_complete[x].body)
