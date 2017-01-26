'''
Based on Bash and Python operators.

Author: jmolle
'''

import logging

from airflow.hooks import AwsLambdaHook #@UnresolvedImport
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class AwsLambdaOperator(BaseOperator):
    """
    Execute a Bash script, command or set of commands.

    :param bash_command: The command, set of commands or reference to a
        bash script (must be '.sh') to be executed.
    :type bash_command: string
    :param env: If env is not None, it must be a mapping that defines the
        environment variables for the new process; these are used instead
        of inheriting the current process environment, which is the default
        behavior. (templated)
    :type env: dict
    :type output_encoding: output encoding of bash command
    """
    template_fields = ('bash_command', 'env')
    template_ext = ('.sh', '.bash',)
    ui_color = '#f0ede4'

    @apply_defaults
    def __init__(
            self,
            event_json,
            function_name,
            aws_lambda_conn_id = 'aws_lambda_default',
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
        result = hook.invoke_function(self.event,
                             self.function_name,
                             self.version,
                             self.invocation_type)
        
        logging.info(str(result))

    def on_kill(self):
        logging.info('Function finished execution')
