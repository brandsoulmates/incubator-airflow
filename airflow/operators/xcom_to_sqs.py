from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.hooks.sqs_hook import SQSHook
import logging


class XcomToSQS(BaseOperator):

    @apply_defaults
    def __init__(self, queue_name=None,
                 xcom_pull=False,
                 context_id=None,
                 *args, **kwargs):

        super(XcomToSQS, self).__init__(*args, **kwargs)
        self.xcom_pull_flag = xcom_pull
        self.queue_name = queue_name
        self.context_id = context_id
        self.kwargs = kwargs

    def execute(self, context):
        if self.xcom_pull_flag:
            self.sh = SQSHook(self.queue_name)
            val = self.xcom_pull(context,
                                 self.context_id,
                                 key='return_value',
                                 include_prior_dates=False)
            if isinstance(val,list):
                self.sh.send_message(','.join(val))
            else:
                logging.info("return value not pushed to sqs: "+str(val))
