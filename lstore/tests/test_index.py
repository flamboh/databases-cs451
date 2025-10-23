import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from lstore.index import Index


class FakeTable:
    """Minimal table stub that exposes the interface Index expects."""

    def __init__(
        self,
        num_columns: int,
        key: int,
        initial_rows: Iterable[Tuple[int, List[Optional[int]]]],
    ) -> None:
        self.num_columns = num_columns
        self.key = key
        self._rows: Dict[int, List[Optional[int]]] = {
            rid: list(columns) for rid, columns in initial_rows
        }

    def iter_rows_for_index(self) -> Iterable[Tuple[int, List[Optional[int]]]]:
        for rid, columns in self._rows.items():
            yield rid, list(columns)

    def add_row(self, rid: int, columns: List[Optional[int]]) -> None:
        self._rows[rid] = list(columns)

    def update_row(self, rid: int, columns: List[Optional[int]]) -> None:
        self._rows[rid] = list(columns)


def test_primary_index_bulk_load_and_lookup():
    table = FakeTable(
        num_columns=2,
        key=0,
        initial_rows=[
            (101, [10, 100]),
            (102, [20, 200]),
            (103, [30, 300]),
        ],
    )

    print("Creating Index for primary key column on existing rows...")
    index = Index(table)

    print("Validating primary-key lookups...")
    assert index.locate(0, 10) == [101]
    assert index.locate(0, 25) == []
    assert index.locate_range(15, 35, 0) == [102, 103]
    print("Ensuring duplicate index creation is rejected...")
    assert not index.create_index(0)


def test_secondary_index_add_update_remove_and_drop():
    table = FakeTable(
        num_columns=3,
        key=0,
        initial_rows=[
            (1, [10, 100, 1000]),
            (2, [20, 200, 2000]),
        ],
    )
    index = Index(table)

    print("Creating secondary index on column 1...")
    assert index.create_index(1)
    print("Checking initial lookups...")
    assert index.locate(1, 100) == [1]
    assert sorted(index.locate(1, 200)) == [2]

    new_row = [30, 200, 3000]
    print("Adding new row with duplicate secondary value...")
    table.add_row(3, new_row)
    index.add(3, new_row)
    assert sorted(index.locate(1, 200)) == [2, 3]

    null_row = [40, None, 4000]
    print("Adding row with None for indexed column...")
    table.add_row(4, null_row)
    index.add(4, null_row)
    assert 4 not in index.locate(1, 200)

    print("Updating row 3 to move it between index buckets...")
    index.update(3, new_row, [30, 250, 3000])
    table.update_row(3, [30, 250, 3000])
    assert 3 not in index.locate(1, 200)
    assert index.locate(1, 250) == [3]

    print("Removing row 2 from the index...")
    index.remove(2, [20, 200, 2000])
    assert index.locate(1, 200) == []

    print("Dropping and recreating the secondary index...")
    assert index.drop_index(1)
    assert index.locate(1, 250) == []

    assert index.create_index(1)
    assert index.locate(1, 100) == [1]
    assert index.locate(1, 250) == [3]
    assert index.drop_index(0) is False


if __name__ == "__main__":
    test_primary_index_bulk_load_and_lookup()
    test_secondary_index_add_update_remove_and_drop()
