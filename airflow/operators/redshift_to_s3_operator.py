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
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from six import string_types


class RedshiftToS3Transfer(BaseOperator):
    """
    Executes an UNLOAD command to s3 as a CSV with headers
    :param schema: reference to a specific schema in redshift database
    :type schema: string
    :param table: reference to a specific table in redshift database
    :type table: string
    :param s3_bucket: reference to a specific S3 bucket
    :type s3_bucket: string
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
            schema,
            table,
            s3_bucket,
            s3_key,
            redshift_conn_id='redshift_default',
            s3_conn_id='aws_default',
            unload_options=tuple(),
            autocommit=False,
            parameters=None,
            custom_select=None,
            wlm_queue=None,
            delimiter="|",
            *args, **kwargs):
        super(RedshiftToS3Transfer, self).__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.redshift_conn_id = redshift_conn_id
        self.s3_conn_id = s3_conn_id
        self.unload_options = unload_options
        self.custom_select = custom_select
        self.autocommit = autocommit
        self.parameters = parameters
        self.wlm_queue = wlm_queue
        self.delimiter = delimiter

    def column_mapping(self, columns):
        ret_val = []
        for a in columns:
            if 'message_text' in a[0]:
                ret_val.append("CAST(REPLACE({0}, chr(10), chr(32)) AS TEXT) AS {0}".format(a[0]))
            elif a[1] == "boolean":
                ret_val.append(
                    "CAST((CASE when {0} then \\'1\\' else \\'0\\' end) AS TEXT) AS {0}".format(a[0], a[1]))
            else:
                ret_val.append("CAST({0} AS TEXT) AS {0}".format(a[0]))
        return ', '.join(ret_val)

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.s3 = S3Hook(s3_conn_id=self.s3_conn_id)
        a_key, s_key = self.s3.get_credentials()
        unload_options = ('\n\t\t\t').join(self.unload_options)

        logging.info("Retrieving headers from %s.%s..." % (self.schema, self.table))

        date_dir = datetime.today().strftime("%Y%m%d")

        # Incase you have a custom SQL
        if self.custom_select:
            unload_query = """
                            UNLOAD ('{0}')
                            TO 's3://{3}/{4}/{8}/{2}/{2}_'
                            with
                            credentials 'aws_access_key_id={5};aws_secret_access_key={6}'
                            {7}
                            delimiter '{9}' addquotes escape allowoverwrite;
                            """.format(self.custom_select, self.schema, self.table,
                                       self.s3_bucket, self.s3_key, a_key, s_key, unload_options, date_dir,
                                       self.delimiter)
        else:
            columns_query = """SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = '{0}'
                    AND   table_name = '{1}'
                    ORDER BY ordinal_position
                """.format(self.schema, self.table)

            cursor = self.hook.get_conn().cursor()
            cursor.execute(columns_query)
            rows = cursor.fetchall()
            column = ', '.join(map(lambda row: row[0], rows))
            columns = map(lambda row: [row[0], row[1]], rows)
            column_names = (', ').join(map(lambda c: "\\'{0}\\'".format(c[0]), columns))
            # (', ').join(map(lambda c: "CAST({0} AS {1}) AS {0}".format(c[0], c[1]),
            column_castings = self.column_mapping(columns)
            #                 columns))

            unload_query = """
                UNLOAD ('SELECT {0}
                UNION
                SELECT {1}
                FROM {2}.{3} order by 1 desc')
                TO 's3://{4}/{5}/{9}/{3}/{3}_'
                with
                credentials 'aws_access_key_id={6};aws_secret_access_key={7}'
                {8}
                delimiter '|' addquotes escape allowoverwrite;
                """.format(column_names, column_castings, self.schema, self.table,
                           self.s3_bucket, self.s3_key, a_key, s_key, unload_options, date_dir)

        # If we are expected to use a worflow management queue
        if isinstance(self.wlm_queue, string_types):
            wlm_prefix = """set query_group to {wlm_queue};
                         """.format(wlm_queue=self.wlm_queue)
            wlm_suffix = """
                         reset query_group;"""
            unload_query = wlm_prefix + unload_query + wlm_suffix

        print(unload_query)
        logging.info('Executing UNLOAD command...')
        self.hook.run(unload_query, self.autocommit)
        logging.info("UNLOAD command complete...")
