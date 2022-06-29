from datetime import datetime
from typing import Dict, Any

from injector import inject, Injector

from src.modules import AwsModule, LocalModule
from src.order_chains import OrderChains
from src.underlyings_queue import UnderlyingsQueue


@inject
def enqueue_underlyings(order_chains: OrderChains, underlyings_queue: UnderlyingsQueue):
  processing_time = datetime.now()
  for underlying in order_chains.get_underlyings():
    underlyings_queue.put(underlying, processing_time)


def lambda_handler(event: Dict = None, context: Any = None) -> None:
  Injector(AwsModule).call_with_injection(enqueue_underlyings)


if __name__ == '__main__':
  Injector(LocalModule).call_with_injection(enqueue_underlyings)
