from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.hooks.aws_sqs_hook3 import AwsSqsHook3
from airflow.exceptions import AirflowException


class SQSOperator3(BaseOperator):

    @apply_defaults
    def __init__(self, 
                 messages = None,
                 num_msgs = 10,
                 chunk_size = None,
                 receipt_handles = None,
                 region_name = None,
                 request_type="receive", # receive, pop, send
                 sqs_queue=None, 
                 aws_lambda_conn_id = 'aws_default',
                 *args, **kwargs):
        
        super(SQSOperator3, self).__init__(*args, **kwargs)
        
        self.sqs_queue_name = sqs_queue
        self.chunk_size = chunk_size
        self.region_name = region_name
        self.aws_lambda_conn_id = aws_lambda_conn_id
        self.request_type = request_type
        self.exec_args = {
                  'message_num':num_msgs,
                  'receipt_handles':receipt_handles,
                  'msgs':messages,
                  'wait_time':5
                  }

    def execute(self, context):
        
        self.sh = AwsSqsHook3(self.sqs_queue_name, sqs_conn_id = self.aws_lambda_conn_id,
                              region_name = self.region_name)
        try:
            resp = getattr(self.sh,self.request_type+"_messages")(**self.exec_args)
            if self.chunk_size:
                cnum = 0
                for resp_chunk in [resp[pos:pos+self.chunk_size] for pos in\
                                   range(0,len(resp),self.chunk_size)]:
                    self.xcom_push(context,"resp"+str(cnum),resp_chunk)
                    cnum+=1
            else:
                return resp
        except AttributeError as ex:
            print(ex)
            raise AirflowException("Improper request type, this only supports"+\
                                   "receive, delete, send, and pop")
