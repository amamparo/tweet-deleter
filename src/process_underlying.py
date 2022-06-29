import json
from itertools import islice
from typing import Dict, Any

import boto3
from injector import Injector, inject

from src.bucket import Bucket
from src.env import Env
from src.modules import LocalModule, AwsModule
from src.order_chains import OrderChains


class ProcessUnderlying:
  @inject
  def __init__(self, order_chains: OrderChains, env: Env, bucket: Bucket):
    self.__order_chains = order_chains
    self.__env = env
    self.__bucket = bucket

  def run(self, underlying: str, export_timestamp: str) -> None:
    key = f'exported_at={export_timestamp}/underlying={underlying.replace("/", "_")}.json'
    with self.__bucket.get_multipart_writer(key) as s3:
      order_chains = self.__order_chains.get_order_chains(underlying)
      while True:
        batch = list(islice(order_chains, 1_000))
        if not batch:
          break
        s3.write('\n'.join(json.dumps(oc.to_flat_dict()) for oc in batch))


def lambda_handler(event: Dict = None, context: Any = None) -> None:
  sqs = boto3.client('sqs')
  for record in event.get('Records', []):
    [account, queue_name] = record['eventSourceARN'].split(':')[-2:]
    queue_url = sqs.get_queue_url(QueueName=queue_name, QueueOwnerAWSAccountId=account)['QueueUrl']
    try:
      Injector(AwsModule).get(ProcessUnderlying).run(record['body'],
                                                     record['messageAttributes']['export_timestamp']['stringValue'])
    finally:
      sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=record['receiptHandle'])


if __name__ == '__main__':
  Injector(LocalModule).get(ProcessUnderlying).run('SPY', 'timestamp')
