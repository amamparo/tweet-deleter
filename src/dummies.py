from datetime import datetime
from io import StringIO
from typing import Iterator

from src.bucket import Bucket
from src.order_chain import OrderChain
from src.order_chains import OrderChains
from src.underlyings_queue import UnderlyingsQueue


class DummyOrderChains(OrderChains):
  def get_underlyings(self) -> Iterator[str]:
    return ['SPY', 'QQQ', 'IWM']

  def get_order_chains(self, underlying: str) -> Iterator[OrderChain]:
    order_chains = [
      OrderChain(
        account_number='foo',
        underlying=underlying,
        description=f'order chain {i}',
        computed_data={
          'foo': 'bar',
          '1': [2, 3],
          'a': {'b': 'c'}
        },
        is_open=True,
        is_winner=True,
        nodes_size=1
      )
      for i in range(10_000)
    ]
    for order_chain in order_chains:
      yield order_chain


class DummyBucket(Bucket):
  def get_multipart_writer(self, path: str) -> StringIO:
    class DummyWriter(StringIO):
      def write(self, data: str):
        print(f'Writing {data} to {path}')

    return DummyWriter()


class DummyUnderlyingsQueue(UnderlyingsQueue):

  def put(self, underlying: str, processing_time: datetime) -> None:
    print(f'Put {underlying} at {self._timestamp(processing_time)}')
