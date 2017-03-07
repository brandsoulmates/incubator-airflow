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


class TwitterLambdaOperator(BaseOperator):
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

    DEFAULT_CONFIG = {
        'task_ids': None,
        'end_point': None,
        'xcom_num': None,
        'payload':  []
    }

    @apply_defaults
    def __init__(
            self,
            function_name,
            config_json,
            aws_lambda_conn_id='aws_default',
            xcom_push=None,
            function_version="$LATEST",
            invocation_type="RequestResponse",
            *args, **kwargs):
        """
        Start by just invoking something.
        args:
        event, function_name, version='$LATEST', invocation_type = 'Event'
        """
        super(TwitterLambdaOperator, self).__init__(*args, **kwargs)

        # Lambdas can't run for more than 5 minutes.
        # This is causing an error
        # self.execution_timeout = min(self.execution_timeout, timedelta(seconds=365))
        self.xcom_push_flag = xcom_push
        self.config_json = config_json
        self.function_name = function_name
        self.invocation_type = invocation_type
        self.aws_lambda_conn_id = aws_lambda_conn_id
        self.function_version = function_version

    def _get_config_json(self, context):
        """
        Get the config from XCOM
        """
        val = self.xcom_pull(context,
                             self.config_json['task_ids'],
                             key=self.config_json.get(
                                 'xcom_num', 'return_value'),
                             include_prior_dates=False)
        return val

    def execute(self, context):
        """
        Execute the lambda function
        """

        if not self.config_json:
            raise AirflowException

        # initialize payload
        self.config_json['payload'] = {}

        # Add payload
        if self.xcom_push_flag:
            self.config_json['payload']['ids'] = self._get_config_json(context)

        hook = AwsLambdaHook(aws_lambda_conn_id=self.aws_lambda_conn_id)
        result = hook.invoke_function(self.config_json,
                                      self.function_name,
                                      self.function_version,
                                      self.invocation_type)
        result_payload = ""
        if self.xcom_push_flag or self.invocation_type == 'RequestResponse':
            try:
                result_payload = result["Payload"].read()
                function_error = result.get("function_error", None)
                if function_error == "Handled" or function_error == "Unhandled.Handled":
                    context['ti'].xcom_push(key="stackTrace", value=json.loads(result_payload))
                    raise AirflowException("Lambda function " +
                                           +str(self.function_name) + " crashed.")
                logging.info(result_payload)
                return result_payload
            except:
                raise AirflowException("Lambda function " +
                                       +str(self.function_name) + " crashed.")
        logging.info(str(result))

    def on_kill(self):
        logging.info('Function finished execution')
