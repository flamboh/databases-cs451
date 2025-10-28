"""Integration tests for Query operations using a real Table."""

from random import randint, sample, seed

from config import Config
from lstore.db import Database
from lstore.query import Query


def _make_grades_table():
    db = Database()
    table = db.create_table("Grades", 5, 0)
    return table, Query(table)


def test_query_integration_insert_update_select_sum():
    table, query = _make_grades_table()
    seed(3562901)

    records = {}
    for _ in range(200):
        key = 90000000 + randint(0, 10_000)
        while key in records:
            key = 90000000 + randint(0, 10_000)
        row = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
        assert query.insert(*row)
        records[key] = row

    for key, expected in records.items():
        result = query.select(key, 0, [1, 1, 1, 1, 1])
        assert len(result) == 1
        assert result[0].columns == expected

    keys = sorted(records.keys())
    for _ in range(25):
        left, right = sorted(sample(range(len(keys)), 2))
        start, end = keys[left], keys[right]
        for column in range(table.num_columns):
            expected_sum = sum(records[key][column] for key in keys[left : right + 1])
            assert query.sum(start, end, column) == expected_sum

    template = [None] * table.num_columns
    for key in keys:
        updates = template[:]
        for column in range(1, table.num_columns):
            updates[column] = records[key][column] + 1
            records[key][column] = updates[column]
        assert query.update(key, *updates)
        latest = query.select_version(key, 0, [1, 1, 1, 1, 1], 0)[0]
        assert latest.columns == records[key]

    for key in keys:
        prior = query.select_version(key, 0, [1, 1, 1, 1, 1], -1)
        assert len(prior) == 1


def test_query_deletion_tombstone_behavior():
    table, query = _make_grades_table()

    for key in range(50):
        row = [key, key + 10, key + 20, key + 30, key + 40]
        assert query.insert(*row)

    for key in range(0, 50, 2):
        assert query.delete(key)

    for key in range(0, 50, 2):
        assert not query.delete(key)

    for key in range(0, 50, 2):
        rid_matches = table.index.locate(table.key, key)
        assert not rid_matches

        assert query.select(key, 0, [1, 1, 1, 1, 1]) == []

    for key in range(0, 50, 2):
        range_id = key // Config.records_per_range
        page_index = (key // Config.records_per_page) % Config.pages_per_range
        slot_index = key % Config.records_per_page
        indirection_page = table.page_directory.page_directory[range_id]["base"][page_index][
            Config.indirection_column
        ]
        assert indirection_page.read(slot_index) == Config.deleted_record_value

    survivors = query.select(1, 0, [1, 1, 1, 1, 1])
    assert len(survivors) == 1
