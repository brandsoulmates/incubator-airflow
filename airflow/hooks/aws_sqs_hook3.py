from airflow.hooks.base_hook import BaseHook
# Will show up under airflow.hooks.PluginHook
import boto3
from airflow.exceptions import AirflowException

class AwsSqsHook3(BaseHook):

    def __init__(self, sqs_queue_name, sqs_conn_id="aws_default", region_name=None):
        self.sqs_conn_id = sqs_conn_id
        self.sqs_conn = self.get_connection(sqs_conn_id)
        self.extra_params = self.sqs_conn.extra_dejson
        self._a_key = self.extra_params['aws_access_key_id']
        self._s_key = self.extra_params['aws_secret_access_key']
        self.region_name = region_name or self.extra_params['region_name']
        self.sqsObj = boto3.resource("sqs", 
                                     region_name = self.region_name,
                                     aws_access_key_id=self._a_key,
                                     aws_secret_access_key=self._s_key )
        if sqs_queue_name:
            self.change_queue(sqs_queue_name)
        else:
            raise AirflowException("Queue name not mentioned.")

    def _chunk_messages(self,messages,message_name = None):
        '''
        formats messages into Entry format and then yields chunks
        '''
        
        if message_name not in ["ReceiptHandle"]:
            message_name = "MessageBody"
        
        entries = [{"Id":str(pos),message_name:messages[pos]} for pos in range(len(messages))]
        for chunk in [entries[pos:pos+10] for pos in range(0,len(entries),10)]:
            yield chunk
    
    def send_messages(self,**kwargs):
        '''
        send an arbitrary number of messages to SQS. Due to how long it can take this may time us out, so we
        report that.
        
        message_name is either ReceiptHandle or MessageBody
        '''
        
        messages = kwargs["messages"]
        
        #maximum batch size is 10 for SQS, so this can be hard-coded.
        queued_ids = 0 
        total_failures = 0
        
        for chunk in self._chunk_messages(messages, "MessageBody"):
            response = self.sqs.send_messages(Entries = chunk)
            
            successes = response.get("Successful",[])
            queued_ids+=len(successes)
            #log successes and failures for now only if we have > 0 failures
            total_failures += len(response.get("Failed",[]))
            
        return (total_failures, queued_ids)
    
    def receive_messages(self,**kwargs):
        '''
        receive a number of messages when required from SQS.
        '''
        
        return self.get_messages(kwargs.get('message_num',10),
                                 kwargs.get('wait_time',5),
                                 do_pop = False)
        
    def pop_messages(self,**kwargs):
        '''
        receive a number of messages when required from SQS.
        '''
        
        return self.get_messages(kwargs.get('message_num',10),
                                 kwargs.get('wait_time',5),
                                 do_pop = True)

    def delete_messages(self,**kwargs):
        # Delete stuff!
        num_deleted = 0
        receipt_handles = kwargs['receipt_handles']
        fails = []
        for chunk in self._chunk_messages(receipt_handles, "ReceiptHandle"):
            
            response = self.sqs.delete_messages(Entries=chunk)
            if len(response.get('Failed',[])) > 0: 
                print(response)
                fails.extend(response.get('Failed',[]))
            num_deleted += len(chunk)
            
        return fails
            
    def change_queue(self,queue_name):
        self.sqs = self.sqsObj.get_queue_by_name(QueueName = queue_name)
        self.sqs_queue_name = queue_name
        if not self.sqs.attributes['ApproximateNumberOfMessages']:
            raise AirflowException("No messages available to collect from queue "+str(self.sqs_queue_name))
        
    def get_messages(self,message_num = 10, wait_time = 5, do_pop = False):
        '''
        pops message_num messages off the queue and returns them. Deletes from the queue.
        
        Dangeresque, only use if there isn't a reasonable way to verify safety before 
        we need to delete
        '''
        
        max_msgs = self.sqs.attributes.get('ApproximateNumberOfMessages',message_num)
        if message_num > max_msgs: message_num = max_msgs
        
        # TODO: FIFO support
        
        receipt_handles = []
        msg_bodies = []
        
        try:
            while (message_num > 0):
                
                messages = self.sqs.receive_messages(MaxNumberOfMessages=min(10,message_num),
                                               WaitTimeSeconds=max(1,wait_time))
                msgs_received = len(messages)
                #Make sure there's something here for us
                if not msgs_received:
                    break

                for msg in messages:
                    msg_bodies.append(msg.body)
                    receipt_handles.append(msg.receipt_handle)
                message_num -= msgs_received
            
            if do_pop:
                self.delete_messages(receipt_handles=receipt_handles)
        except AirflowException as ex:
            if not len(msg_bodies):
                raise ex
        
        if do_pop:
            # We deleted the msgs, no need to save the receipt handles.
            return msg_bodies
        else:
            # Same len, no need to take the min
            return [(msg_bodies[i],receipt_handles[i]) for i in range(len(msg_bodies))]
        
            
