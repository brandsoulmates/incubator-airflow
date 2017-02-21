'''
Based on Bash and Python operators.

Author: jmolle
'''

import logging

from airflow.hooks.aws_lambda_hook import AwsLambdaHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import timedelta
from airflow.exceptions import AirflowException
import json


class AwsLambdaOperator(BaseOperator):
    """
    Execute a Bash script, command or set of commands.

    :param event_json: The json that we're going to pass to the lambda function.
    :type event_json: dict
    :param function_name: The name of the function being executed.
    :type function_name: string
    :param version: The version or alias of the function to run.
    :type version: string
    :param invocation_type: The type of callback we expect.

        Eventually I'd like to make this more invisible, so the operator can launch sets
        of functions.

    :type invocation_type: string
    """

    ui_color = '#f0ede4'

    DEFAULT_CONFIG = {
        'task_ids': None,
        'end_point': None,
        'xcom_num': None,
        'payload':  []
    }

    @apply_defaults
    def __init__(
            self,
            # function_name,
            config_json,
            aws_lambda_conn_id='aws_default',
            xcom_push=None,
            *args, **kwargs):
        """
        Start by just invoking something.
        args:
        event, function_name, version='$LATEST', invocation_type = 'Event'
        """
        super(AwsLambdaOperator, self).__init__(*args, **kwargs)

        # Lambdas can't run for more than 5 minutes.
        # self.execution_timeout = min(self.execution_timeout, timedelta(seconds=310))
        self.xcom_push_flag = xcom_push
        self.config_json = config_json

        # self.function_name = function_name
        self.aws_lambda_conn_id = aws_lambda_conn_id

    def _get_config_json(self, context):
        """
        Get the config from XCOM
        """
        logging.info("\n==============================\n")
        logging.info(self.config_json.get(
            self.config_json['xcom_num'], 'return_value'))
        logging.info("\n==============================\n")
        logging.info(self.config_json['task_ids'])
        logging.info("\n==============================\n")
        val = self.xcom_pull(context,
                             self.config_json['task_ids'],
                             key=self.config_json.get(
                                 'xcom_num', 'return_value'),
                             include_prior_dates=False)
        logging.info(val)
        logging.info("\n==============================\n")
        return val

    def execute(self, context):
        """
        Execute the lambda function
        """

        if not self.config_json:
            raise AirflowException

        logging.info("getting config")
        logging.info(self.config_json["xcom_num"])
        # initialize payload
        self.config_json['payload'] = {}
        # Add payload
        self.config_json['payload']['ids'] = self._get_config_json(context)
        logging.info(self.config_json)

        # self.event = self.config_json["event_json"]
        # if not self.function_name:
        #     self.function_name = self.config_json["function_name"]
        # self.version = self.config_json.get("version", '$LATEST')
        # self.invocation_type = self.config_json.get("invocation_type", "RequestResponse")

        # logging.info('Invoking lambda function ' + self.function_name +
        #              ' with version ' + self.version)
        # hook = AwsLambdaHook(aws_lambda_conn_id=self.aws_lambda_conn_id)
        # result = hook.invoke_function(self.event,
        #                               self.function_name,
        #                               self.version,
        #                               self.invocation_type)
        # try:
        #     result_payload = result["Payload"].read()
        # except:
        #     result_payload = ""
        # logging.info(self.invocation_type)
        # logging.info(str(result))
        # logging.info(json.loads(result_payload))
        # if self.xcom_push_flag or self.invocation_type == 'RequestResponse':
        #     return json.loads(result_payload)

    def on_kill(self):
        logging.info('Function finished execution')
