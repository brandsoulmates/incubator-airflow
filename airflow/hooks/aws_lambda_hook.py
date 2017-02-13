# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import division
#from future import standard_library
#standard_library.install_aliases()
from six import string_types
from yaml import load
import json
import logging
import boto3
from botocore.exceptions import ClientError

boto3.set_stream_logger('boto3')
logging.getLogger("boto3").setLevel(logging.INFO)

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook

def _parse_lambda_config(config_filename):
    
    with open(config_filename,'r') as yamlfile:
        config_dict = load(yamlfile)
    return config_dict['aws_access_key_id'], config_dict['aws_secret_access_key']

class AwsLambdaHook(BaseHook):
    """
    Interact with Λ. This class is a wrapper around the boto library.
    """
    def __init__(self, aws_lambda_conn_id='aws_default'):
        self.aws_lambda_conn_id = aws_lambda_conn_id
        self.aws_lambda_conn = self.get_connection(aws_lambda_conn_id)
        self.extra_params = self.aws_lambda_conn.extra_dejson
        self.profile = self.extra_params.get('profile')
        self._creds_in_conn = 'aws_secret_access_key' in self.extra_params
        self._creds_in_config_file = 'aws_config_file' in self.extra_params
        if self._creds_in_conn:
            self._a_key = self.extra_params['aws_access_key_id']
            self._s_key = self.extra_params['aws_secret_access_key']
        elif self._creds_in_config_file:
            self.lambda_config_file = self.extra_params['aws_config_file']
        else:
            raise AirflowException("No AWS credentials supplied, no access to Lambda.")
        self.connection = self.get_conn()

    def __getstate__(self):
        pickled_dict = dict(self.__dict__)
        del pickled_dict['connection']
        return pickled_dict

    def __setstate__(self, d):
        self.__dict__.update(d)
        self.__dict__['connection'] = self.get_conn()

    @staticmethod
    def package_event(event):
        """
        packages json-compliant dicts into something that can be fed into a lambda call
        """
        
        # For now, we just serialize it.
        try:
            return json.dumps(event)
        except:
            raise AirflowException("event dict unable to be serialized as JSON!")

    def get_conn(self):
        """
        Returns the boto lambda connection object.
        """
        a_key = s_key = None
        if self._creds_in_config_file:
            a_key, s_key = _parse_lambda_config(self.lambda_config_file)
        elif self._creds_in_conn:
            a_key = self._a_key
            s_key = self._s_key

        connection = boto3.client('lambda',
            aws_access_key_id=a_key,
            aws_secret_access_key=s_key            )
        return connection

    def invoke_function(self, event, function_name, version, invocation_type):
        """
        invokes a lambda function with the event object as the passed event.
        """
        result = None
        kwargs = {'FunctionName':function_name,
                  'InvocationType':invocation_type,
                  'Payload':self.package_event(event)}
        
        if isinstance(version, string_types) and version != '$LATEST':
            kwargs['Qualifier'] = version
            
        try:
            result = self.connection.invoke(**kwargs)
        except ClientError as ex:
            raise AirflowException(str(ex))
        return result
    