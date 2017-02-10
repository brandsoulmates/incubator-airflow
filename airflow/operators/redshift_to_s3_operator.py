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
            s3_conn_id='s3_default',
            unload_options=tuple(),
            autocommit=False,
            parameters=None,
            *args, **kwargs):
        super(RedshiftToS3Transfer, self).__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.redshift_conn_id = redshift_conn_id
        self.s3_conn_id = s3_conn_id
        self.unload_options = unload_options
        self.autocommit = autocommit
        self.parameters = parameters

    def column_mapping(self, columns):
        ret_val = []
        for a in columns:
            if a[1] == "boolean":
                ret_val.append("CAST((CASE when {0} then \\'1\\' else \\'0\\' end) AS TEXT) AS {0}".format(a[0], a[1]))
            else:
                ret_val.append("CAST({0} AS TEXT) AS {0}".format(a[0]))
        return ', '.join(ret_val)
                
    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.s3 = S3Hook(s3_conn_id=self.s3_conn_id)
        a_key, s_key = self.s3.get_credentials()
        unload_options = ('\n\t\t\t').join(self.unload_options)

        logging.info("Retrieving headers from %s.%s..." % (self.schema, self.table))

        columns_query = """SELECT column_name, data_type
                            FROM information_schema.columns
                            WHERE table_schema = '{0}'
                            AND   table_name = '{1}'
                            ORDER BY ordinal_position
                        """.format(self.schema, self.table)

        cursor = self.hook.get_conn().cursor()
        cursor.execute(columns_query)
        rows = cursor.fetchall()
        print rows
        column = ', '.join(map(lambda row: row[0], rows))
        columns = map(lambda row: [row[0], row[1]], rows)
        column_names = (', ').join(map(lambda c: "\\'{0}\\'".format(c[0]), columns))
        column_castings = self.column_mapping(columns) # (', ').join(map(lambda c: "CAST({0} AS {1}) AS {0}".format(c[0], c[1]),
        #                 columns))

        date_dir = datetime.today().strftime("%Y%m%d")
        
        unload_query = """
                        UNLOAD ('SELECT {0}
                        UNION 
                        SELECT {1}
                        FROM {2}.{3}')
                        TO 's3://{4}/{5}/{9}/{3}/{3}_'
                        with
                        credentials 'aws_access_key_id={6};aws_secret_access_key={7}'
                        {8}
                        delimiter '|' addquotes escape allowoverwrite;
                        """.format(column_names, column_castings, self.schema, self.table,
                                   self.s3_bucket, self.s3_key, a_key, s_key, unload_options, date_dir)
        print unload_query
        logging.info('Executing UNLOAD command...')
        self.hook.run(unload_query, self.autocommit)
        logging.info("UNLOAD command complete...")




# UNLOAD ('SELECT \'post_id\', \'channel_id\', \'post_title\', \'message_text\', \'d_date\', \'post_type\', \'updated_date\', \'embedded_media_url\', \'post_image_url\', \'facebook_object_id\', \'timeline_visibility\', \'is_published\', \'like_count\', \'comment_count\', \'share_count\', \'scraped_date\', \'uploaded_date\', \'row_num\'
#                         UNION
#                         SELECT CAST(post_id AS character varying) AS post_id, CAST(channel_id AS character varying) AS channel_id, CAST(post_title AS character varying) AS post_title, CAST(message_text AS character varying) AS message_text, CAST(created_date AS text) AS created_date, CAST(post_type AS character varying) AS post_type, CAST(updated_date AS text) AS updated_date, CAST(embedded_media_url AS character varying) AS embedded_media_url, CAST(post_image_url AS character varying) AS post_image_url, CAST(facebook_object_id AS text) AS facebook_object_id, CAST(timeline_visibility AS character varying) AS timeline_visibility, CAST((CASE when is_published then \'1\' else \'0\' end) as text) AS is_published, CAST(like_count AS text) AS like_count, CAST(comment_count AS text) AS comment_count, CAST(share_count AS text) AS share_count, CAST(scraped_date AS text) AS scraped_date, CAST(uploaded_date AS text) AS uploaded_date, CAST(row_num AS text) AS row_num
#                         FROM brandsoulmates.view_facebook_post_latest')
# TO 's3://ayz-users/brandsoulmates/20170210/view_facebook_post_latest/view_facebook_post_latest_'
# with
# credentials 'aws_access_key_id=AKIAI6637BNKS4AMVIHA;aws_secret_access_key=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'

# delimiter '|' addquotes escape allowoverwrite;
