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

import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from six import string_types


class CopyS3ToRedshift(BaseOperator):
    """
    Executes an COPY command to s3 as a CSV with headers
    :param schema: reference to a specific schema in redshift database
    :type schema: string
    :param table: reference to a specific table in redshift database
    :type table: string
    :param s3_key: reference to a specific S3 key
    :type s3_key: string
    :param redshift_conn_id: reference to a specific redshift database
    :type redshift_conn_id: string
    :param s3_conn_id: reference to a specific S3 connection
    :type s3_conn_id: string
    :param options: reference to a list of UNLOAD options
    :type options: list
    """

    template_fields = ()
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            schema_name,
            table_name,
            s3_path = None,
            s3_path_xcom_ti = None,
            redshift_conn_id='redshift_default',
            s3_conn_id='s3_default',
            region='us-west-2',
            file_format = 'CSV',
            quotechar = '"',
            delimchar = ',',
            load_options=tuple(),
            autocommit=False,
            parameters=None,
            wlm_queue=None,
            *args, **kwargs):
        super(CopyS3ToRedshift, self).__init__(*args, **kwargs)
        
        self.s3_path_xcom_ti = s3_path_xcom_ti        
        self.schema_name = schema_name
        self.table_name = table_name
        self.region = region
        self.s3_path = s3_path
        self.redshift_conn_id = redshift_conn_id
        self.s3_conn_id = s3_conn_id
        self.load_options = load_options
        self.autocommit = autocommit
        self.parameters = parameters
        self.file_format = file_format
        self.quotechar = quotechar
        self.delimchar = delimchar
        self.wlm_queue = wlm_queue

    def _get_s3_path(self,context):
        if self.s3_path:
            return self.s3_path
        elif self.s3_path_xcom_ti:
            return self.xcom_pull(context,
                                  key='return_value',
                                  task_ids=self.s3_path_xcom_ti,
                                  include_prior_dates=False)
        else:
            raise AirflowException("No S3 path to COPY from!")

    def _assign_to_query_group(self,unload_query):
        '''
        If we want to be part of a query group, set that here
        '''
        if isinstance(self.wlm_queue, string_types):
            wlm_prefix = """set query_group to {wlm_queue};
                         """.format(wlm_queue=self.wlm_queue)
            wlm_suffix = """
                         reset query_group;"""
            unload_query = wlm_prefix + unload_query + wlm_suffix
        else:
            raise AirflowException("wlm_queue must be a string!")
        return unload_query
    
    def _build_query(self,a_key,s_key,s3_path,load_options):
        return """
        COPY {schema_name}.{table_name} FROM '{s3_path}' 
        CREDENTIALS 'aws_access_key_id={aws_key};aws_secret_access_key={aws_secret}'
        REGION '{region}' DELIMITER '{delimchar}' FORMAT {file_format} QUOTE AS '{quotechar}'
        {load_options} ;
        """.format(schema_name = self.schema_name, 
                   table_name = self.table_name,
                   s3_path = s3_path, 
                   aws_key = a_key,
                   aws_secret = s_key,
                   region = self.region,
                   delimchar = self.delimchar,
                   quotechar = self.quotechar,
                   file_format = self.file_format,
                   load_options = load_options
                   )

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.s3 = S3Hook(s3_conn_id=self.s3_conn_id)
        a_key, s_key = self.s3.get_credentials()
        load_options = (' ').join(self.load_options)

        s3_path = self._get_s3_path(context)
        s3_parts = s3_path.split("s3://")[-1].split("/",1)
        bucket_name = s3_parts[0]
        prefix = s3_parts[1]
        
        
        if self.s3.check_for_prefix(bucket_name, prefix, "/"):
            # Build the query
            unload_query = self._build_query(a_key, s_key, s3_path, load_options)
            
            # Add to a WLM queue, if desired
            if self.wlm_queue:
                self._assign_to_query_group(unload_query)
                
            logging.info('Executing COPY command...')
            self.hook.run(unload_query, self.autocommit)
            logging.info("COPY command complete...")
        else:
            logging.info("Skipping COPY command, no files to copy.")
