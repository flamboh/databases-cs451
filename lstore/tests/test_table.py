import pytest
from random import randint, seed, sample

from config import Config
from lstore.table import Table


def test_random_key_insertions():
    grades_table = Table("grades", num_columns=5, key=0)
    stored_records = {}

    record_count = 100
    seed(3562901)

    keys = sample(range(92106429, 92106429 + record_count * 3), record_count)

    for primary_key in keys:
        base_record = (
            [-1 for _ in range(Config.base_meta_columns)]
            + [primary_key]
            + [randint(0, 20) for _ in range(grades_table.num_columns - 1)]
        )
        stored_records[primary_key] = base_record
        grades_table.insert_record(base_record)

    for record_index in range(record_count):
        range_id = record_index // Config.records_per_range
        offset = record_index % Config.records_per_range
        rid = grades_table.page_directory.encode_rid(range_id, 0, offset)

        record = grades_table.get_record(rid)
        assert record == stored_records[keys[record_index]]


def test_delete_record():
    grades_table = Table("grades", num_columns=5, key=0)

    record_count = 100
    seed(3562901)

    keys = sample(range(92106429, 92106429 + record_count * 3), record_count)

    for primary_key in keys:
        record_values = (
            [-1 for _ in range(Config.base_meta_columns)]
            + [primary_key]
            + [randint(0, 20) for _ in range(grades_table.num_columns - 1)]
        )
        grades_table.insert_record(record_values)

    page_directory = grades_table.page_directory

    for record_index in range(record_count):
        assert grades_table.delete_record(record_index)

        with pytest.raises(RuntimeError):
            grades_table.get_record(record_index)

        range_id = record_index // Config.records_per_range
        page_index = (record_index // Config.records_per_page) % Config.pages_per_range
        slot_index = record_index % Config.records_per_page

        indirection_page = page_directory.page_directory[range_id]["base"][page_index][Config.indirection_column]
        assert indirection_page.read(slot_index) == Config.deleted_record_value

    assert not grades_table.delete_record(0)


def test_insert_tail_record():
    grades_table = Table("grades", num_columns=5, key=0)
    base_and_tail_records = {}

    record_count = 100
    seed(3562901)

    keys = sample(range(92106429, 92106429 + record_count * 3), record_count)
    base_meta_template = [Config.null_value for _ in range(Config.base_meta_columns)]

    for primary_key in keys:
        tail_record = base_meta_template + [-1] + [primary_key] + [
            Config.null_value if (value := randint(0, 20)) % 3 == 0 else value
            for _ in range(grades_table.num_columns - 1)
        ]
        base_record = (
            base_meta_template
            + [primary_key]
            + [randint(0, 20) for _ in range(grades_table.num_columns - 1)]
        )
        base_and_tail_records[primary_key] = {"base": base_record, "tail": tail_record}

    for primary_key in keys:
        base_rid = grades_table.insert_record(base_and_tail_records[primary_key]["base"], is_tail=False)
        tail_record = base_and_tail_records[primary_key]["tail"]
        tail_record[Config.base_rid_column] = base_rid
        grades_table.insert_record(tail_record, is_tail=True, base_rid=base_rid)

    for record_index in range(record_count):
        range_id = record_index // Config.records_per_range
        offset = record_index % Config.records_per_range
        rid = grades_table.page_directory.encode_rid(range_id, 0, offset)
        updated_record = grades_table.get_relative_version_of_record(rid, -1)
        assert updated_record[Config.base_rid_column] == rid

    # Insert a second wave of tails to exercise chaining behaviour.
    for primary_key in keys:
        secondary_tail = base_meta_template + [-1] + [primary_key] + [
            Config.null_value if (value := randint(0, 20)) % 2 == 0 else value
            for _ in range(grades_table.num_columns - 1)
        ]
        base_rid = base_and_tail_records[primary_key]["tail"][Config.base_rid_column]
        secondary_tail[Config.base_rid_column] = base_rid
        grades_table.insert_record(secondary_tail, is_tail=True, base_rid=base_rid)


def test_tail_range_capacity_limit():
    grades_table = Table("grades", num_columns=5, key=0)
    base_meta_template = [Config.null_value for _ in range(Config.base_meta_columns)]

    base_record = base_meta_template + [42] + [0 for _ in range(grades_table.num_columns - 1)]
    base_rid = grades_table.insert_record(base_record, is_tail=False)

    # Fill all tail slots in the base range.
    for tail_index in range(Config.records_per_range):
        tail_record = base_meta_template + [-1] + [42] + [tail_index for _ in range(grades_table.num_columns - 1)]
        tail_record[Config.base_rid_column] = base_rid
        grades_table.insert_record(tail_record, is_tail=True, base_rid=base_rid)

    overflowing_tail = base_meta_template + [-1] + [42] + [99 for _ in range(grades_table.num_columns - 1)]
    overflowing_tail[Config.base_rid_column] = base_rid

    with pytest.raises(RuntimeError):
        grades_table.insert_record(overflowing_tail, is_tail=True, base_rid=base_rid)


def test_large_base_insert_spans_ranges():
    grades_table = Table("grades", num_columns=5, key=0)

    total_records = Config.records_per_range * 2
    base_meta_template = [Config.null_value for _ in range(Config.base_meta_columns)]

    for record_index in range(total_records):
        primary_key = 1_000_000 + record_index
        base_record = (
            base_meta_template
            + [primary_key]
            + [record_index % 10 for _ in range(grades_table.num_columns - 1)]
        )
        grades_table.insert_record(base_record, is_tail=False)

    for record_index in range(total_records):
        range_id = record_index // Config.records_per_range
        offset = record_index % Config.records_per_range
        rid = grades_table.page_directory.encode_rid(range_id, 0, offset)
        record = grades_table.get_record(rid)
        assert record[Config.rid_column] == rid
