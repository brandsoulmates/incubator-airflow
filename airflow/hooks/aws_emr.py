import logging
import traceback
from airflow.hooks.base_hook import BaseHook
# Will show up under airflow.hooks.PluginHook
import boto3
import tempfile
import json
from airflow.hooks.S3_hook import S3Hook
from subprocess import Popen, STDOUT, PIPE


class EMRHook(BaseHook):
    STATES = ['TERMINATED', 'TERMINATED_WITH_ERRORS', 'RUNNING', 'WAITING']

    def __init__(self, emr_conn_id="aws_default", region="us-west-2"):
        self.region_name = region
        self.emr_conn_id = emr_conn_id
        self.emr_conn = self.get_connection(emr_conn_id)
        self.extra_params = self.emr_conn.extra_dejson
        self._a_key = self.extra_params['aws_access_key_id']
        self._s_key = self.extra_params['aws_secret_access_key']
        self._get_conn()

    def _get_conn(self):
        self.emr = boto3.client(
            'emr',
            aws_access_key_id=self._a_key,
            region_name=self.region_name,
            aws_secret_access_key=self._s_key)
        self.s3 = S3Hook()

    def list_clusters(self, cluster_id=None):
        # if list_of_ids:
        #     loi = []
        #     for elem in self.emr.list_clusters()['Clusters']:
        #         loi.append(elem[u'Id'])
        #     return loi
        return self.emr.list_clusters()

    def describe_cluster(self, job_id):
        return self.emr.describe_cluster(ClusterId=job_id)

    def isWaiting(self, job_id):
        json_dict = self.describe_cluster(job_id)
        if json_dict['Cluster']['Status']['State'] == 'WAITING':
            return True
        return False

    def isTerminated(self, job_id):
        json_dict = self.describe_cluster(job_id)
        if json_dict['Cluster']['Status']['State'] == 'TERMINATED':
            return True
        return False

    def isTermError(self, job_id):
        json_dict = self.describe_cluster(job_id)
        if json_dict['Cluster']['Status']['State'] == 'TERMINATED_WITH_ERRORS':
            return True
        return False

    def isRunning(self, job_id):
        json_dict = self.describe_cluster(job_id)
        if json_dict['Cluster']['Status']['State'] == 'RUNNING':
            return True
        return False

    def isSuccessfull(self, job_id):
        if self.isTerminated(job_id):
            return True
        return False

    def run_the_job(self, bucket, key):
        tf = tempfile.NamedTemporaryFile()
        self.s3.download_file(bucket, key, tf.name)
        sp = Popen(
            ['bash', open(tf.name, 'r').read()], stdout=PIPE, stderr=PIPE)
        out, err = sp.communicate()
        json_out = json.loads(out)
        return json_out['id']

    def terminate_job(self, job_id):
        """
        You can only terminate one instance with this.
        Thats enough ain't it. Don't go too crazy
        """
        try:
            self.emr.terminate_job_flows(JobFlowIds=[job_id])
            return True
        except:
            return False
