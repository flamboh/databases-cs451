import pytest
from random import randint, seed, sample
from time import perf_counter

from config import Config
from lstore.table import Table


def _build_base_record(num_columns, primary_key, *values):
    assert len(values) == num_columns - 1
    return [Config.null_value for _ in range(Config.base_meta_columns)] + [primary_key, *values]


def _build_tail_record(num_columns, updates):
    tail_record = [Config.null_value for _ in range(Config.tail_meta_columns + num_columns)]
    for column_index, value in updates.items():
        tail_record[Config.tail_meta_columns + column_index] = value
    return tail_record


def test_random_key_insertions():
    grades_table = Table("grades", num_columns=5, key=0)
    stored_records = {}

    record_count = 100
    seed(3562901)

    keys = sample(range(92106429, 92106429 + record_count * 3), record_count)
    start_time = perf_counter()

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

    duration = perf_counter() - start_time
    print(f"random key insertions took {duration:.2f}s")
    assert duration < 30.0, f"random key insertions took {duration:.2f}s"


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

        tombstoned_record = grades_table.get_record(record_index)
        assert tombstoned_record[Config.indirection_column] == Config.deleted_record_value

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
    if Config.max_tail_segments > 8:
        print("Skipping tail range capacity limit test because max_tail_segments is greater than 8")
        return
    grades_table = Table("grades", num_columns=5, key=0)
    base_meta_template = [Config.null_value for _ in range(Config.base_meta_columns)]

    base_record = base_meta_template + [42] + [0 for _ in range(grades_table.num_columns - 1)]
    base_rid = grades_table.insert_record(base_record, is_tail=False)

    # Fill all tail slots in the base range across every tail segment.
    tail_capacity = Config.records_per_range * Config.max_tail_segments
    for tail_index in range(tail_capacity):
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


def test_merge_range_promotes_tail_updates_to_base_pages():
    grades_table = Table("grades", num_columns=3, key=0)
    base_rid = grades_table.insert_record(
        _build_base_record(grades_table.num_columns, 100, 10, 20),
        is_tail=False,
    )

    tail_record = _build_tail_record(grades_table.num_columns, {1: 77})
    grades_table.insert_record(tail_record, is_tail=True, base_rid=base_rid)

    pre_merge_data = grades_table.get_record(base_rid)[
        Config.base_meta_columns : Config.base_meta_columns + grades_table.num_columns
    ]
    assert pre_merge_data[1] == 10

    grades_table.page_directory.merge_range(0)

    merged_data = grades_table.get_record(base_rid)[
        Config.base_meta_columns : Config.base_meta_columns + grades_table.num_columns
    ]
    assert merged_data == [100, 77, 20]


def test_merge_range_updates_every_base_record_in_range():
    grades_table = Table("grades", num_columns=4, key=0)
    base_rows = [
        _build_base_record(grades_table.num_columns, 100, 10, 20, 30),
        _build_base_record(grades_table.num_columns, 200, 40, 50, 60),
    ]
    base_rids = [grades_table.insert_record(row, is_tail=False) for row in base_rows]

    tail_updates = [
        (base_rids[0], {1: 90}),
        (base_rids[0], {2: 123}),
        (base_rids[1], {3: 70}),
        (base_rids[1], {3: 80}),
    ]
    for rid, update_map in tail_updates:
        tail_record = _build_tail_record(grades_table.num_columns, update_map)
        grades_table.insert_record(tail_record, is_tail=True, base_rid=rid)

    grades_table.page_directory.merge_range(0)

    expected_by_rid = {
        base_rids[0]: [100, 90, 123, 30],
        base_rids[1]: [200, 40, 50, 80],
    }
    for rid, expected in expected_by_rid.items():
        merged_data = grades_table.get_record(rid)[
            Config.base_meta_columns : Config.base_meta_columns + grades_table.num_columns
        ]
        assert merged_data == expected


def test_merge_threshold_triggers_after_configured_tail_count():
    grades_table = Table("grades", num_columns=3, key=0)
    base_rid = grades_table.insert_record(
        _build_base_record(grades_table.num_columns, 100, 10, 20),
        is_tail=False,
    )
    directory = grades_table.page_directory
    directory.merge_thresholds[0] = 1

    def base_data():
        return grades_table.get_record(base_rid)[
            Config.base_meta_columns : Config.base_meta_columns + grades_table.num_columns
        ]

    def insert_tail(value):
        tail_record = _build_tail_record(grades_table.num_columns, {1: value})
        grades_table.insert_record(tail_record, is_tail=True, base_rid=base_rid)

    assert base_data() == [100, 10, 20]

    insert_tail(55)
    assert base_data() == [100, 10, 20]

    insert_tail(65)
    assert base_data() == [100, 55, 20]
    assert directory.merge_thresholds[0] == 2


def test_merge_threshold_requires_additional_updates_after_each_merge():
    grades_table = Table("grades", num_columns=3, key=0)
    base_rid = grades_table.insert_record(
        _build_base_record(grades_table.num_columns, 100, 10, 20),
        is_tail=False,
    )
    directory = grades_table.page_directory
    directory.merge_thresholds[0] = 1

    def base_data():
        return grades_table.get_record(base_rid)[
            Config.base_meta_columns : Config.base_meta_columns + grades_table.num_columns
        ]

    def insert_tail(value):
        tail_record = _build_tail_record(grades_table.num_columns, {1: value})
        grades_table.insert_record(tail_record, is_tail=True, base_rid=base_rid)

    insert_tail(50)
    insert_tail(60)
    assert base_data() == [100, 50, 20]
    assert directory.merge_thresholds[0] == 2

    insert_tail(70)
    assert base_data() == [100, 50, 20]

    insert_tail(80)
    assert base_data() == [100, 70, 20]
    assert directory.merge_thresholds[0] == 4
