from abc import ABC, abstractmethod
from datetime import datetime

import boto3
from injector import inject

from src.env import Env


class UnderlyingsQueue(ABC):
  @abstractmethod
  def put(self, underlying: str, processing_time: datetime) -> None:
    pass

  def _timestamp(self, time: datetime) -> str:
    return time.isoformat()


class SqsUnderlyingsQueue(UnderlyingsQueue):
  @inject
  def __init__(self, env: Env):
    self.__queue = boto3.resource('sqs').Queue(env.queue_url)

  def put(self, underlying: str, processing_time: datetime) -> None:
    self.__queue.send_message(
      MessageBody=underlying,
      MessageAttributes={
        'export_timestamp': {
          'StringValue': self._timestamp(processing_time),
          'DataType': 'String'
        }
      }
    )


