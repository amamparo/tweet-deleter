from injector import Binder, singleton, Module

from src.bucket import Bucket, S3Bucket
from src.order_chains import OrderChains, RdsOrderChains
from src.dummies import DummyOrderChains, DummyBucket, DummyUnderlyingsQueue
from src.underlyings_queue import UnderlyingsQueue, SqsUnderlyingsQueue


class AwsModule(Module):
  def configure(self, binder: Binder) -> None:
    binder.bind(OrderChains, to=RdsOrderChains, scope=singleton)
    binder.bind(UnderlyingsQueue, to=SqsUnderlyingsQueue, scope=singleton)
    binder.bind(Bucket, to=S3Bucket, scope=singleton)


class LocalModule(Module):
  def configure(self, binder: Binder) -> None:
    binder.bind(OrderChains, to=DummyOrderChains, scope=singleton)
    binder.bind(UnderlyingsQueue, to=DummyUnderlyingsQueue, scope=singleton)
    binder.bind(Bucket, to=DummyBucket, scope=singleton)
