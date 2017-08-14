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

import os
from airflow.hooks.S3_hook import S3Hook

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class S3ToFileSystem(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            s3_bucket,
            s3_key,
            download_file_location,
            s3_conn_id='aws_default',
            * args, **kwargs):

        super(S3ToFileSystem, self).__init__(*args, **kwargs)

        self.local_location = download_file_location
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_conn_id = s3_conn_id

    def execute(self, context):
        self.s3 = S3Hook(s3_conn_id=self.s3_conn_id)
        file_paths = []
        for k in self.s3.list_keys(self.s3_bucket, prefix=self.s3_key):
            kpath = os.path.join(self.local_location, os.path.basename(k))
            # Download the file
            self.s3.download_file(self.s3_bucket, k, kpath)
            file_paths.append(kpath)
            context['ti'].xcom_push(key=kpath, value="")
        context['ti'].xcom_push(key="files_added", value=file_paths)
        # read in chunks
        # start reading from the file.
        # insert in respective SQS operators
