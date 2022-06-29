import time
from abc import ABC, abstractmethod
from math import floor
from typing import Iterator

import boto3
import psycopg2
from injector import inject
from psycopg2._psycopg import cursor
from psycopg2.extras import RealDictCursor

from src.env import Env
from src.order_chain import OrderChain


class OrderChains(ABC):
  @abstractmethod
  def get_underlyings(self) -> Iterator[str]:
    pass

  @abstractmethod
  def get_order_chains(self, underlying: str) -> Iterator[OrderChain]:
    pass


class RdsOrderChains(OrderChains):

  @inject
  def __init__(self, env: Env):
    self.__env = env
    self.__conn = psycopg2.connect(
      database='order_chains', user=env.pg_user, host=env.pg_host, port=5432, cursor_factory=RealDictCursor,
      password=boto3.client('rds').generate_db_auth_token(
        DBHostname=env.pg_host,
        Port=5432,
        DBUsername=env.pg_user,
        Region=env.region
      )
    )

  def get_underlyings(self) -> Iterator[str]:
    query = f"""
      select distinct(underlying_symbol)
      from order_chains.order_chains
      where last_occurred_at >= now() - interval '{self.__env.past_n_years_window} years'
    """
    with self.__execute(query) as cur:
      for record in cur:
        yield record['underlying_symbol']

  def get_order_chains(self, underlying: str) -> Iterator[OrderChain]:
    query = f"""
      select account_number, underlying_symbol as underlying, description, computed_data, is_open, is_winner, nodes_size
      from order_chains.order_chains
      where underlying_symbol='{underlying}'
        and last_occurred_at >= now() - interval '{self.__env.past_n_years_window} years'
    """
    with self.__execute(query) as cur:
      for record in cur:
        yield OrderChain(
          account_number=record['account_number'],
          underlying=record['underlying'],
          description=record['description'],
          computed_data=record['computed_data'],
          is_open=record['is_open'],
          is_winner=record['is_winner'],
          nodes_size=record['nodes_size']
        )

  def __execute(self, query: str) -> cursor:
    curs = self.__conn.cursor(f'cursor-{floor(time.time())}')
    curs.itersize = self.__env.pg_batch_size
    curs.execute(query)
    return curs


