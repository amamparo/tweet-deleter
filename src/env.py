from dataclasses import dataclass
from os import environ

from injector import singleton


@dataclass
@singleton
class Env:
  pg_host = environ.get('PG_HOST')
  pg_user = environ.get('PG_USER')
  pg_batch_size = int(environ.get('PG_BATCH_SIZE', 2_000))
  region = environ.get('AWS_REGION')
  queue_url = environ.get('QUEUE_URL')
  past_n_years_window = int(environ.get('PAST_N_YEARS_WINDOW', 1))
  output_bucket = environ.get('OUTPUT_BUCKET')
