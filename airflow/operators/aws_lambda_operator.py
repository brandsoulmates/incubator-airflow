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
import copy


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
            warn_on_error=False,
            *args, **kwargs):
        """
        Start by just invoking something.
        args:
        event_json, function_name, function_version='$LATEST', invocation_type = 'Event',
        event_xcoms, xcom_push
        """
        super(AwsLambdaOperator, self).__init__(*args, **kwargs)
        # Lambdas can't run for more than 5 minutes.
        # Should be at least 10 seconds longer than lambda function timeout.
        self.num_invocations = 1
        if event_xcoms:
            self.num_invocations = len(event_xcoms)
        if not isinstance(self.execution_timeout,timedelta):
            self.execution_timeout = timedelta(seconds=340)
        self.execution_timeout = timedelta(seconds=(max(min(\
                                self.execution_timeout.seconds,340),13)*\
                                                    self.num_invocations))
        self.xcom_push_flag = xcom_push
        self.event_xcoms = event_xcoms
        self.event_json = event_json
        self.function_name = function_name
        self.aws_lambda_conn_id = aws_lambda_conn_id
        self.invocation_type = invocation_type
        self.function_version = function_version
        self.warn_on_error = warn_on_error

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
            if isinstance(xcom_dict[key], dict) and\
                    isinstance(event_dict[key], dict) and\
                    (key in event_dict):
                self._add_dict_to_event_(event_dict[key], xcom_dict[key])
            else:
                event_dict[key] = xcom_dict[key]

    def _load_xcoms_into_event(self, context, xcom_index, event_json):
        """
        Get the config from XCOM
        """

        task_xcom = self.event_xcoms[xcom_index]
        xcom_result = self.xcom_pull(context,
                                     task_xcom['task_id'],
                                     key=task_xcom.get('key', 'return_value'),
                                     include_prior_dates=False)
        if isinstance(xcom_result, string_types):
            xcom_result = json.loads(xcom_result)
        
        if isinstance(xcom_result, dict):
            self._add_dict_to_event_(event_json, xcom_result)
        else:
            logging.error(xcom_result)
            # This is only an error if it is the first message.
            # Otherwise we skip the execution.
            if xcom_index > 0:
                raise AirflowException("Unable to read XCom from " +\
                                       str(task_xcom) + ", improper format " +\
                                       str(type(xcom_result)))
            else:
                return
        return event_json

    def execute(self, context):
        """
        Execute the lambda function
        """
        num_crashes = 0
        for i in range(self.num_invocations):
            ej = copy.deepcopy(self.event_json)
            if self.event_xcoms:
                ej = self._load_xcoms_into_event(context, i, ej)
            # Don't run a lambda without an xcom if we were promised one.
            if not ej:
                continue
    
            logging.info(ej)
            logging.info('Invoking lambda function ' + str(self.function_name) +
                         ' with version ' + str(self.function_version))
            logging.info(self.invocation_type)
    
            hook = AwsLambdaHook(aws_lambda_conn_id=self.aws_lambda_conn_id)
            result = hook.invoke_function(ej,
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
                    self.xcom_push(context, 'payload_json_'+str(i),
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
                if self.num_invocations > 1 and self.warn_on_error:
                    logging.warning("Lambda function " +\
                                    str(self.function_name) +\
                                    " crashed!")
                    num_crashes += 1
                else:
                    raise AirflowException("Lambda function " +\
                                           str(self.function_name) +\
                                           " crashed!")
        if self.num_invocations <= 1:
            return result_json
        elif num_crashes >= (self.num_invocations/2):
            raise AirflowException("Excessive crashes for function "+\
                                   str(self.function_name))

    def on_kill(self):
        logging.info('Function finished execution')
