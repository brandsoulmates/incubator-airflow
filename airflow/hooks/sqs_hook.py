import logging
import traceback
from airflow.hooks.base_hook import BaseHook
# Will show up under airflow.hooks.PluginHook
import boto3


class SQSHook(BaseHook):

    def __init__(self, queue=None,
                 sqs_conn_id="s3_default",
                 region="us-west-2"):
        self.region = region
        self.sqs_conn_id = sqs_conn_id
        self.sqs_conn = self.get_connection(sqs_conn_id)
        self.extra_params = self.sqs_conn.extra_dejson
        self._a_key = self.extra_params['aws_access_key_id']
        self._s_key = self.extra_params['aws_secret_access_key']

        if queue:
            self.queue = queue
            self._get_conn()
        else:
            print("Queue name not mentioned.")

    def _get_conn(self):
        self.sqs = boto3.resource('sqs',
                                  aws_access_key_id=self._a_key,
                                  aws_secret_access_key=self._s_key)
        # initialize queue
        self.q = self.sqs.get_queue_by_name(QueueName=self.queue)

    def len_queue(self):
        return self.q.count()

    def send_message(self, msg):
        self.q.send_message(MessageBody=msg)

    def receive_messages(self):
        rs = self.q.receive_messages()
        m = rs[0]
        return m

    def receive_bulk(self, num, delete_on_recieve):
        count = 0
        messages = []
        while(True):
            try:
                rs = self.q.receive_messages(MaxNumberOfMessages=num)
                count = count + len(rs)
                messages.append(rs)
                if delete_on_recieve:
                    self.delete_messages(rs)
                if count >= num:
                    break
            except:
                logging.error(traceback.print_exc())
                raise Exception
        flatten = sum(messages, [])
        return flatten

    def delete_messages(self, m):
        for msg in m:
            msg.delete()
