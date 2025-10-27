"""Index manager backed by a B+ tree per indexed column."""

from __future__ import annotations

from typing import Iterable, List, Optional, Tuple, TYPE_CHECKING

from config import Config
from lstore.bplus import BPlusTree

if TYPE_CHECKING:
    from lstore.table import Table

class Index:
    """Maintains secondary structures to accelerate column lookups."""

    def __init__(self, table: "Table") -> None:
        self.table = table
        self.indices: List[Optional[BPlusTree]] = [None] * table.num_columns
        # Always build an index for the primary key column.
        self.create_index(table.key)

    # ------------------------------------------------------------------
    # Lookup helpers
    # ------------------------------------------------------------------
    def locate(self, column: int, value: int) -> List[int]:
        tree = self.indices[column]
        if tree is None:
            return []
        return tree.find(value)

    def locate_range(self, begin: int, end: int, column: int) -> List[int]:
        tree = self.indices[column]
        if tree is None:
            return []
        return tree.find_range(begin, end)

    # ------------------------------------------------------------------
    # Mutation helpers
    # ------------------------------------------------------------------
    def add(self, rid: int, columns: List[Optional[int]]) -> None:
        for column, tree in enumerate(self.indices):
            if tree is None:
                continue
            value = columns[column]
            if value is None:
                continue
            tree.insert(value, rid)

    def remove(self, rid: int, columns: List[Optional[int]]) -> None:
        for column, tree in enumerate(self.indices):
            if tree is None:
                continue
            value = columns[column]
            if value is None:
                continue
            tree.remove(value, rid)

    def update(self, rid: int, old_values: List[Optional[int]], new_values: List[Optional[int]]) -> None:
        for column, tree in enumerate(self.indices):
            if tree is None:
                continue
            old = old_values[column]
            new = new_values[column]
            if old == new or old is None or new is None:
                continue
            tree.remove(old, rid)
            tree.insert(new, rid)

    # ------------------------------------------------------------------
    # Index lifecycle
    # ------------------------------------------------------------------
    def create_index(self, column_number: int) -> bool:
        pass
        if not 0 <= column_number < self.table.num_columns:
            raise ValueError(f"Column {column_number} out of bounds for index creation")
        if self.indices[column_number] is not None:
            return False

        tree = BPlusTree()
        self.indices[column_number] = tree
        self._bulk_load(column_number, tree)
        return True

    def drop_index(self, column_number: int) -> bool:
        pass
        if column_number == self.table.key:
            # Primary key index must always exist.
            return False
        if self.indices[column_number] is None:
            return False
        self.indices[column_number] = None
        return True

    # ------------------------------------------------------------------
    # Bulk loading helpers
    # ------------------------------------------------------------------
    def _bulk_load(self, column_number: int, tree: BPlusTree) -> None:
        pass
        for rid, row in self._iterate_existing_rows():
            try:
                value = row[column_number]
            except IndexError:
                continue
            if value is None:
                continue
            tree.insert(value, rid)

    def _iterate_existing_rows(self) -> Iterable[Tuple[int, List[Optional[int]]]]:
        pass
        if hasattr(self.table, "iter_rows_for_index"):
            yield from self.table.iter_rows_for_index()
            return

        directory = getattr(self.table, "page_directory", None)
        get_record = getattr(self.table, "get_record", None)
        if directory is None or get_record is None:
            return

        # If the table exposes a page directory, iterate known RIDs.
        for rid in range(directory.num_base_records):
            try:
                record = get_record(rid)
            except RuntimeError:
                continue
            yield rid, record[Config.base_meta_columns :]
