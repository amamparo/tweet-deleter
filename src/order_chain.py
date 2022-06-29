from dataclasses import dataclass


@dataclass
class OrderChain:
  account_number: str
  underlying: str
  description: str
  computed_data: dict
  is_open: bool
  is_winner: bool
  nodes_size: int

  def to_flat_dict(self) -> dict:
    as_dict = dict(self.__dict__)
    for key, value in self.computed_data.items():
      if isinstance(value, list) or isinstance(value, dict):
        continue
      as_dict[key] = value
    del as_dict['computed_data']
    return as_dict
