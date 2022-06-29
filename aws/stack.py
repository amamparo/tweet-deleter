from os import environ, path, getcwd
from typing import cast, Dict, List, Optional, Any

from aws_cdk.aws_ec2 import SecurityGroup, Vpc, Port, SubnetType, SubnetSelection
from aws_cdk.aws_events import Rule, Schedule
from aws_cdk.aws_events_targets import LambdaFunction
from aws_cdk.aws_iam import PolicyStatement, Effect, Role, IPrincipal, ServicePrincipal, PolicyDocument, ManagedPolicy
from aws_cdk.aws_lambda import DockerImageFunction, DockerImageCode
from aws_cdk.aws_sqs import Queue
from aws_cdk.core import Stack, Construct, App, Environment, Duration
from dotenv import dotenv_values
from aws_cdk.aws_lambda_event_sources import SqsEventSource

schedule = Schedule.cron(hour='5', minute='0', week_day='SUN')
past_n_years_window = 1
concurrency = 16

environment = environ.get('ENVIRONMENT')
config = dotenv_values(path.join(getcwd(), 'env', f'{environment}.env'))


class OrderChainsDataExportStack(Stack):
  def __init__(self, scope: Construct) -> None:
    super().__init__(scope, 'OrderChainsDataExport',
                     env=Environment(account=config['ACCOUNT'], region='us-east-2'))
    vpc = Vpc.from_lookup(self, 'vpc', vpc_id=config['VPC_ID'])
    sg = SecurityGroup(self, 'security-group', vpc=vpc)
    SecurityGroup.from_lookup_by_name(
      self, 'db-security-group', 'order-chains-database', vpc
    ).add_ingress_rule(sg, Port.tcp(5432), 'data export')
    queue = Queue(self, 'Queue', queue_name='order-chains-data-export-underlyings',
                  retention_period=Duration.days(1), visibility_timeout=Duration.minutes(15),
                  receive_message_wait_time=Duration.seconds(20))
    self.__create_enqueue_underlyings_function(vpc, sg, queue)
    self.__create_process_underlying_function(vpc, sg, queue)

  def __create_enqueue_underlyings_function(self, vpc: Vpc, sg: SecurityGroup, queue: Queue) -> None:
    function = LambdaFunction(self.__create_function(
      'enqueue-underlyings',
      'src.enqueue_underlyings.lambda_handler',
      vpc, sg,
      memory_size=1024,
      env={
        'PG_BATCH_SIZE': 1_000,
        'QUEUE_URL': queue.queue_url
      },
      policy_statements=[
        self.__allow('sqs:SendMessage', f'arn:aws:sqs:{self.region}:{self.account}:{queue.queue_name}')
      ]
    ))
    Rule(self, 'schedule', schedule=schedule).add_target(function)

  def __create_process_underlying_function(self, vpc: Vpc, sg: SecurityGroup, queue: Queue) -> None:
    output_bucket = config['OUTPUT_BUCKET']
    process_underlying_function = self.__create_function(
      'process-underlying',
      'src.process_underlying.lambda_handler',
      vpc, sg,
      max_concurrency=concurrency,
      memory_size=8192,
      env={
        'PG_BATCH_SIZE': 10_000,
        'OUTPUT_BUCKET': output_bucket
      },
      policy_statements=[self.__allow('s3:PutObject', f'arn:aws:s3:::{output_bucket}/*')],
      role_name=f'{environment}-DumpOrderChainsDataRole'
    )
    process_underlying_function.add_event_source(SqsEventSource(queue, batch_size=1))

  def __create_function(self, name: str, handler_path: str, vpc: Vpc, sg: SecurityGroup,
                        env: Optional[Dict[str, Any]] = None, policy_statements: Optional[List[PolicyStatement]] = None,
                        max_concurrency: Optional[int] = 1, memory_size: Optional[int] = 128,
                        role_name: Optional[str] = None) -> DockerImageFunction:
    pg_user = 'iam_order_chains'
    shared_policy_statements = [
      self.__allow('rds-db:connect', f'arn:aws:rds-db:{self.region}:{self.account}:dbuser:*/{pg_user}')
    ]
    shared_env = {
      'PG_HOST': config['PG_HOST'],
      'PG_USER': pg_user,
      'PAST_N_YEARS_WINDOW': past_n_years_window
    }
    return DockerImageFunction(
      self,
      f'{name}-function',
      reserved_concurrent_executions=max_concurrency,
      function_name=name,
      description=self.stack_name,
      memory_size=memory_size,
      code=DockerImageCode.from_image_asset(
        directory=getcwd(),
        file='Dockerfile',
        exclude=['cdk.out', 'env', 'aws'],
        cmd=[handler_path]
      ),
      timeout=Duration.minutes(15),
      allow_public_subnet=False,
      vpc=vpc,
      vpc_subnets=SubnetSelection(subnet_type=SubnetType.PRIVATE),
      security_groups=[sg],
      environment={k: str(v) for k, v in {**shared_env, **(env or {})}.items()},
      role=Role(
        self,
        f'{name}-role',
        role_name=role_name or f'{environment}-{name}-role',
        assumed_by=cast(IPrincipal, ServicePrincipal('lambda.amazonaws.com')),
        managed_policies=[
          ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaVPCAccessExecutionRole'),
          ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole')
        ],
        inline_policies={'Policies': PolicyDocument(statements=shared_policy_statements + (policy_statements or []))}
      )
    )

  @staticmethod
  def __allow(action: str, resource: str) -> PolicyStatement:
    return PolicyStatement(effect=Effect.ALLOW, actions=[action], resources=[resource])


if __name__ == '__main__':
  app = App()
  OrderChainsDataExportStack(app)
  app.synth()
