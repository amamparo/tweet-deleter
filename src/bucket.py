from abc import ABC, abstractmethod
from io import StringIO

from injector import inject
from smart_open import open

from src.env import Env


class Bucket(ABC):
  @abstractmethod
  def get_multipart_writer(self, path: str) -> StringIO:
    pass


class S3Bucket(Bucket):
  @inject
  def __init__(self, env: Env):
    self.__env = env

  def get_multipart_writer(self, path: str) -> StringIO:
    return open(f's3://{self.__env.output_bucket}/{path}', 'w')


