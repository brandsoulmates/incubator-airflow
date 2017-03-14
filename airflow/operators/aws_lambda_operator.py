'''
Based on Bash and Python operators.

Author: jmolle
'''

import logging

from airflow.hooks.aws_lambda_hook import AwsLambdaHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import timedelta
import json
from airflow.exceptions import AirflowException
from six import string_types


class AwsLambdaOperator(BaseOperator):
    """
    Execute a Bash script, command or set of commands.

    :param event_json: The json that we're going to pass to the lambda function.
    :type event_json: dict
    :param function_name: The name of the function being executed.
    :type function_name: string
    :param function_version: The version or alias of the function to run.
    :type function_version: string
    :param invocation_type: The type of callback we expect.

        Eventually I'd like to make this more invisible, so the operator can launch sets
        of functions.
    :type invocation_type: string
    """

    ui_color = '#f0ede4'

    @apply_defaults
    def __init__(
            self,
            function_name,
            function_version="$LATEST",
            invocation_type="RequestResponse",
            event_xcoms=None,
            event_json={},
            aws_lambda_conn_id='aws_default',
            xcom_push=True,
            *args, **kwargs):
        """
        Start by just invoking something.
        args:
        event_json, function_name, function_version='$LATEST', invocation_type = 'Event',
        event_xcoms, xcom_push
        """
        super(AwsLambdaOperator, self).__init__(*args, **kwargs)
        # Lambdas can't run for more than 5 minutes.
        self.execution_timeout = min(self.execution_timeout, timedelta(seconds=365))
        self.xcom_push_flag = xcom_push
        self.event_xcoms = event_xcoms
        self.event_json = event_json
        self.function_name = function_name
        self.aws_lambda_conn_id = aws_lambda_conn_id
        self.invocation_type = invocation_type
        self.function_version = function_version

    def _add_dict_to_event_(self, event_dict, xcom_dict):
        """
        Adds the xcom message to the existing event.
        Recursive. Oooga Booga.
            - Passes pointers to the current position in both objects.
            - DFS graph traversal: nobody wants to make a duplicate stack.
            - Runs in time N, where N is the total number of keys in xcom_dict.
        """
        for key in xcom_dict:
            # exceptions in this case are half the speed, so I'm writing ugly code.
            if isinstance(xcom_dict, dict) and isinstance(event_dict, dict) and\
                    (key in event_dict):
                self._add_dict_to_event_(event_dict[key], xcom_dict[key])
            else:
                event_dict[key] = xcom_dict[key]

    def _load_xcoms_into_event(self, context):
        """
        Get the config from XCOM
        """

        for task_xcom in self.event_xcoms:
            xcom_result = self.xcom_pull(context,
                                         task_xcom['task_id'],
                                         key=task_xcom.get('key', 'return_value'),
                                         include_prior_dates=False)
            if isinstance(xcom_result, string_types):
                xcom_result = json.loads(xcom_result)
            elif isinstance(xcom_result, dict):
                self._add_dict_to_event_(self.event_json, xcom_result)
            else:
                logging.error(xcom_result)
                raise AirflowException("Unable to read XCom from " + str(task_xcom) +
                                       ", improper format " + str(type(xcom_result)))

    def execute(self, context):
        """
        Execute the lambda function
        """
        if self.event_xcoms:
            self._load_xcoms_into_event(context)

        logging.info(self.event_json)
        logging.info('Invoking lambda function ' + str(self.function_name) +
                     ' with version ' + str(self.function_version))
        logging.info(self.invocation_type)

        hook = AwsLambdaHook(aws_lambda_conn_id=self.aws_lambda_conn_id)
        result = hook.invoke_function(self.event_json,
                                      self.function_name,
                                      self.function_version,
                                      self.invocation_type)
        result_payload = ""
        result_json = {}
        
        # Push if there is an error, regardless of what we planned on doing.
        self.xcom_push_flag = self.xcom_push_flag or ("FunctionError" in result)
        
        for key in result:
            logging.debug(key, result[key])
            if self.xcom_push_flag:
                self.xcom_push(context, key, result[key])
        try:
            result_payload = result["Payload"].read()
            result_json = json.loads(result_payload)
            logging.info(result_payload)
            if self.xcom_push_flag:
                self.xcom_push(context, 'payload_json',
                               json.loads(result_payload))
        except:
            if self.invocation_type == 'RequestResponse':
                # Errors always return jsons,
                # so this won't ruin error-checking
                raise AirflowException("Lambda function " +\
                                   str(self.function_name) +\
                                   " returned an invalid object type.")
        
        if "FunctionError" in result:
            # This function crashed, throw an error
            raise AirflowException("Lambda function " +\
                                   str(self.function_name) +\
                                   " crashed!")
            
        return result_json

    def on_kill(self):
        logging.info('Function finished execution')
