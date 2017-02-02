'''
Based on Bash and Python operators.

Author: jmolle
'''

import logging

from airflow.hooks.aws_lambda_hook import AwsLambdaHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import time

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

    @apply_defaults
    def __init__(
            self,
            event_json,
            function_name,
            aws_lambda_conn_id = 'aws_default',
            version=None,
            invocation_type = None,
            *args, **kwargs):
        """
        Start by just invoking something.
        args:
        event, function_name, version='$LATEST', invocation_type = 'Event'
        """
        super(AwsLambdaOperator, self).__init__(*args, **kwargs)
        self.event = event_json
        self.function_name = function_name
        self.version = version if version else '$LATEST'
        self.invocation_type = invocation_type if invocation_type else 'Event'
        self.aws_lambda_conn_id = aws_lambda_conn_id

    def execute(self, context):
        """
        Execute the bash command in a temporary directory
        which will be cleaned afterwards
        """
        logging.info('Invoking lambda function '+self.function_name+\
                     ' with version '+self.version)
        hook = AwsLambdaHook(aws_lambda_conn_id = self.aws_lambda_conn_id)
        time.sleep(10)
        result = hook.invoke_function(self.event,
                             self.function_name,
                             self.version,
                             self.invocation_type)
        
        logging.info(str(result))
        return result

    def on_kill(self):
        logging.info('Function finished execution')
