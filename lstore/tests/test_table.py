import pytest
from config import Config
from lstore.table import Table
from random import randint, seed, sample

def test_table_add_record_prints_page_directory():
    table = Table("students", num_columns=5, key=0)
    metadata = [0] * Config.base_meta_columns
    payload = [11, 22, 33, 44, 55][: table.num_columns]

    try:
        table.insert_record(metadata + payload)
    except Exception as exc:  # pragma: no cover - surface unexpected failure for debugging
        pytest.fail(f"add_record raised {exc!r}")

    # print(table.page_directory.page_directory)

    # print(table.get_record(0))


def test_random_key_insertions():
  table = Table("grades", num_columns=5, key=0)
  records = {}

  number_of_records = 10000
  seed(3562901)

  keys = sample(range(92106429, 92106429 + number_of_records * 3), number_of_records)

  for key in keys:
      rec = [-1 for _ in range(Config.base_meta_columns)] + [key] + [randint(0, 20) for _ in range(table.num_columns - 1)]
      records[key] = rec
      table.insert_record(rec)
  
  for i in range(number_of_records):
    record = table.get_record(i)
    assert record[Config.rid_column] == i