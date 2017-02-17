import logging
from airflow.hooks.base_hook import BaseHook
# Will show up under airflow.hooks.PluginHook
from boto.sqs import connect_to_region
from boto.sqs.message import Message


class SQSHook(BaseHook):

    def __init__(self, queue=None, sqs_conn_id="s3_default", region="us-west-2"):
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
        logging.info(self._a_key)
        logging.info(self._s_key)
        self.conn = connect_to_region(self.region, aws_access_key_id=self._a_key,
                                      aws_secret_access_key=self._s_key)
        self.q = self.conn.create_queue(self.queue)

    def len_queue(self):
        return self.q.count()

    def check_for_queue(self, queue):
        return self.conn.get_queue(queue)

    def list_queues(self):
        return self.conn.get_all_queues()

    def send_message(self, msg):
        m = Message()
        m.set_body(msg)
        self.q.write(m)

    def receive_messages(self):
        rs = self.q.get_messages()
        m = rs[0]
        return m

    def delete_messages(self, m):
        self.q.delete_messages(m)
