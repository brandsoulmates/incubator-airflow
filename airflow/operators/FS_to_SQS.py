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
from os import listdir
from os.path import isfile, join

from airflow.hooks.sqs_hook import SQSHook

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class FileSystemToSQS(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            queue_names,
            download_file_location,
            max_size=100,
            * args, **kwargs):

        super(FileSystemToSQS, self).__init__(*args, **kwargs)
        self.max_size = max_size
        self.local_location = download_file_location
        self.queue_names = queue_names

    def execute(self, context):
        queues = []
        for qn in self.queue_names:
            queues.append(SQSHook(queue=qn))

        onlyfiles = [f for f in listdir(self.local_location)
                     if isfile(join(self.local_location, f))]
        # Get all files in the directory
        for ifn in onlyfiles:
            fn = join(self.local_location, ifn)
            # Read the file
            with open(fn, "r") as ins:
                # Populate array to max length
                array = []
                for line in ins:
                    array.append(line.strip())
                    # Insert into SQS Queue
                    if len(array) == self.max_size:
                        for q in queues:
                            q.send_message(','.join(array))
                        # Remove all messages which are inserted
                        del array[:]
