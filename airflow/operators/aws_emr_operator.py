import logging
import shlex
import subprocess
import json

from airflow.hooks.aws_emr import EMRHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from slackclient import SlackClient
from time import sleep
import os


class AwsEMROperator(BaseOperator):
    ui_color = '#00BFFF'
    @apply_defaults
    def __init__(
            self,
            event_xcoms=None,
            aws_emr_conn_id='aws_default',
            xcom_push=True,
            command_args=[[]],
            channel="#airflow_notifications",
            download_these_files=[],
            start_cluster=False,
            terminate_cluster=False,
            xcom_task_id="job_runner",
            dn_dir="./tmp",
            username='airflow',
            method='chat.postMessage',
            icon_url='https://raw.githubusercontent.com/airbnb/airflow/master/airflow/www/static/pin_100.png',
            *args,
            **kwargs):
        """
        Start by just invoking something.
        """
        super(AwsEMROperator, self).__init__(*args, **kwargs)
        self.channel = channel
        self.username = username
        self.icon_url = icon_url
        self.download_these_files = download_these_files
        self.conn_id = aws_emr_conn_id
        self.method = 'chat.postMessage'
        self.command_args = command_args
        self.start_cluster = start_cluster
        self.terminate_cluster = terminate_cluster
        self.dn_dir = dn_dir

    def slack_message(self, text):
        self.token = os.environ["SLACK_API_TOKEN"]
        sc = SlackClient(self.token)
        api_params = {
            'channel': self.channel,
            'username': self.username,
            'text': text,
            'icon_url': self.icon_url,
            'link_names': 1
        }
        sc.api_call(self.method, **api_params)

    def construct_command(self):
        command = "aws emr create-cluster"
        for key, value in self.command_args:
            command = command + " " + key + " " + value
        logging.info("Command is: " + command)
        return shlex.split(command)

    def exec_command(self, command):
        p = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()

        if stderr != b'':
            logging.info("Non zero exit code.")
            logging.info(stderr)
            raise AirflowException("The return code is non zero: " +
                                   stderr)

        output = json.loads(stdout.replace("\n", ""))["ClusterId"]
        logging.info("output_id: " + output)
        return output

    def execute(self, context):
        s3_hook = S3Hook()
        for bucket, key in self.download_these_files:
            basename = os.path.basename(key)
            s3_hook.download_file(bucket, key, os.path.join(self.dn_dir, basename))

        job_monitor = EMRHook(emr_conn_id="S3_default")
        if self.start_cluster:
            output_id = self.exec_command(self.construct_command())
            context['ti'].xcom_push(key="code", value=output_id)
        if self.terminate_cluster:

            output_id = context['ti'].xcom_pull(
                task_id=self.xcom_task_id, key="code")
            self.slack_message("""
                @channel\n ----------------------------------------\nThe Cluster is being terminated for this job. \n ----------------------------------------\nProcess id = %s
                """ % output_id)
            if not job_monitor.isTerminated(output_id):
                job_monitor.terminate_job(output_id)
                return
            else:
                return
        self.slack_message("""
        @channel
        The task Id of the new job is: %s
        """ %
            output_id)
        while True:
            if job_monitor.isRunning(output_id):
                sleep(900)
            elif job_monitor.isSuccessfull(output_id):
                self.slack_message("""
                @channel\n ----------------------------------------\nThe process is Successful.\n Manual check is always a good thing. \n ----------------------------------------\nProcess id = %s
                """ % output_id)
                break
            elif job_monitor.isTermError(output_id):
                self.slack_message("""
                @channel\n ----------------------------------------\nThe process Terminated with Errors\n ----------------------------------------\nProcess id = %s
                """ % output_id)
                raise AirflowException("The process is terminated with Errors")
            elif job_monitor.isTerminated(output_id):
                self.slack_message("""
                @channel\n ----------------------------------------\nThe process has been terminated\n ----------------------------------------\nProcess id = %s
                """ % output_id)
                raise AirflowException("The process is terminated")

            elif job_monitor.isWaiting(output_id):
                self.slack_message("""
                @channel\n ----------------------------------------\nThe process is WAITING\n ----------------------------------------\nProcess id = %s
                """ % output_id)
                raise AirflowException(
                    "Somebody needs to go see whats up. Spark Job is in  Waiting State for id: %s" % output_id)
            else:
                sleep(300)

# def slack_message():
#     token = os.environ["SLACK_API_TOKEN"]
#     sc = SlackClient(token)
#     api_params = {
#         'channel': "airflow_notifications",
#         'username': "airflow",
#         'text': "ssup @channel",
#         'icon_url': 'https://raw.githubusercontent.com/airbnb/airflow/master/airflow/www/static/pin_100.png',
#         'link_names': 1
#     }
#     sc.api_call("chat.postMessage", **api_params)
