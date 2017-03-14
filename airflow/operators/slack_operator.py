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
from slackclient import SlackClient
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
import json
import logging


class SlackAPIOperator(BaseOperator):
    """
    Base Slack Operator
    The SlackAPIPostOperator is derived from this operator.
    In the future additional Slack API Operators will be derived from this class as well

    :param token: Slack API token (https://api.slack.com/web)
    :type token: string
    :param method: The Slack API Method to Call (https://api.slack.com/methods)
    :type method: string
    :param api_params: API Method call parameters (https://api.slack.com/methods)
    :type api_params: dict
    """

    @apply_defaults
    def __init__(self,
                 token='unset',
                 method='unset',
                 api_params=None,
                 task_xcom=None,
                 xcom_pull=False,
                 *args, **kwargs):
        super(SlackAPIOperator, self).__init__(*args, **kwargs)
        self.token = os.environ["SLACK_API_TOKEN"]
        self.method = method
        self.task_xcom = task_xcom
        self.xcom_pull_flag = xcom_pull
        self.api_params = api_params

    def construct_api_call_params(self):
        """
        Used by the execute function. Allows templating on the source fields of the api_call_params dict before construction

        Override in child classes.
        Each SlackAPIOperator child class is responsible for having a construct_api_call_params function
        which sets self.api_call_params with a dict of API call parameters (https://api.slack.com/methods)
        """

        pass

    def execute(self, **kwargs):
        """
        SlackAPIOperator calls will not fail even if the call is not unsuccessful.
        It should not prevent a DAG from completing in success
        """
        if not self.api_params:
            self.construct_api_call_params()

        if self.xcom_pull_flag and self.task_xcom:
            self.api_params["text"] = self.xcom_pull(kwargs.get("context", "Default_Value"),
                           self.task_xcom,
                           key='return_value',
                           include_prior_dates=False)
            
        sc = SlackClient(self.token)
        rc = sc.api_call(self.method, **self.api_params)
        
        if not rc['ok']:
            logging.error("Slack API call failed ({})".format(rc['error']))
            raise AirflowException("Slack API call failed: ({})".format(rc['error']))


class SlackAPIPostOperator(SlackAPIOperator):
    """
    Posts messages to a slack channel

    :param channel: channel in which to post message on slack name (#general) or ID (C12318391)
    :type channel: string
    :param username: Username that airflow will be posting to Slack as
    :type username: string
    :param text: message to send to slack
    :type text: string
    :param icon_url: url to icon used for this message
    :type icon_url: string
    :param attachments: extra formatting details - see https://api.slack.com/docs/attachments
    :type attachments: array of hashes
    """

    template_fields = ('username', 'text', 'attachments')
    ui_color = '#FFBA40'

    @apply_defaults
    def __init__(self,
                 channel='#general',
                 username='Airflow',
                 text='No message has been set.\n'
                      'Here is a cat video instead\n'
                      'https://www.youtube.com/watch?v=J---aiyznGQ',
                 icon_url='https://raw.githubusercontent.com/airbnb/airflow/master/airflow/www/static/pin_100.png',
                 attachments=None,
                 xcom_pull=False,
                 task_xcom=None,
                 *args, **kwargs):
        self.method = 'chat.postMessage'
        self.channel = channel
        self.username = username
        self.text = text
        self.icon_url = icon_url
        self.attachments = attachments
        self.xcom_pull_flag = xcom_pull
        self.task_xcom = task_xcom
        super(SlackAPIPostOperator, self).__init__(method=self.method,
                                                   xcom_pull=self.xcom_pull_flag,
                                                   task_xcom=self.task_xcom,
                                                   *args, **kwargs)

    def construct_api_call_params(self):
        self.api_params = {
            'channel': self.channel,
            'username': self.username,
            'text': self.text,
            'icon_url': self.icon_url,
            'attachments': json.dumps(self.attachments),
        }
