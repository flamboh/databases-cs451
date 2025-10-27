import pytest
from config import Config
from lstore.table import Table
from random import randint, seed, sample


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
    range_id = i // Config.records_per_range
    offset = i % Config.records_per_range
    rid = table.page_directory.encode_rid(range_id, 0, offset)
    record = table.get_record(rid)
    assert record == records[keys[i]]

  
def test_delete_record():
  table = Table("grades", num_columns=5, key=0)
  records = {}

  number_of_records = 10
  seed(3562901)

  keys = sample(range(92106429, 92106429 + number_of_records * 3), number_of_records)

  for key in keys:
    rec = [-1 for _ in range(Config.base_meta_columns)] + [key] + [randint(0, 20) for _ in range(table.num_columns - 1)]
    records[key] = rec
    table.insert_record(rec)

  for i in range(number_of_records):
    assert table.delete_record(i)
    record = table.get_record(i)
    assert record[Config.rid_column] == Config.null_value
  assert table.delete_record(0)


def test_insert_tail_record():
  table = Table("grades", num_columns=5, key=0)
  records = {}

  number_of_records = 10000
  seed(3562901)

  keys = sample(range(92106429, 92106429 + number_of_records * 3), number_of_records)
  base_meta = [Config.null_value for _ in range(Config.base_meta_columns)]
  for key in keys:
    rec = base_meta + [key] + [Config.null_value if (v := randint(0, 20)) == 7 else v for _ in range(table.num_columns - 1)]
    records[key] = rec

  for key in keys:
    base_rid = table.insert_record(records[key], is_tail=False)
    tail_rec = base_meta + [base_rid] + records[key][Config.base_meta_columns:]
    table.insert_record(tail_rec, is_tail=True, base_rid=base_rid)

  for i in range(number_of_records):
    range_id = i // Config.records_per_range
    offset = i % Config.records_per_range
    rid = table.page_directory.encode_rid(range_id, 0, offset)
    tail_record = table.get_version_of_record(rid, 0)
    base_record = table.get_record(tail_record[Config.base_rid_column])
  print("test", tail_record)
  print("scehma encoding", bin(tail_record[Config.schema_encoding_column]))
  print("base record", base_record)
  print("scehma encoding", bin(base_record[Config.schema_encoding_column]))