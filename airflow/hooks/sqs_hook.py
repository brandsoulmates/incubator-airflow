from airflow.hooks.base_hook import BaseHook
# Will show up under airflow.hooks.PluginHook
from boto.sqs import connect_to_region
from boto.sqs.message import Message


class SQSHook(BaseHook):

    def __init__(self, queue=None, region="us-west-2"):
        self.region = region
        if queue:
            self.queue = queue
            self._get_conn()
        else:
            print("Queue name not mentioned.")

    def _get_conn(self):
        self.conn = connect_to_region(self.region, aws_access_key_id="AKIAI6637BNKS4AMVIHA",
                                      aws_secret_access_key="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")
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
        return m.get_body()

    def delete_messages(self, m):
        self.q.delete_messages(m)
