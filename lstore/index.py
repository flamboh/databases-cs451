"""Index manager backed by a B+ tree per indexed column."""

from __future__ import annotations

from typing import Iterable, List, Optional, Tuple

from lstore.bplus import BPlusTree

from lstore.table import Table

class Index:
    """Maintains secondary structures to accelerate column lookups."""

    def __init__(self, table) -> None:
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
        raise NotImplementedError('create_index not implemented yet')
        if self.indices[column_number] is not None:
            return False

        tree = BPlusTree()
        self.indices[column_number] = tree
        self._bulk_load(column_number, tree)
        return True

    def drop_index(self, column_number: int) -> bool:
        raise NotImplementedError('drop_index not implemented yet')
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
    ''' def _bulk_load(self, column_number: int, tree: BPlusTree) -> None:
        for rid, row in self._iterate_existing_rows():
            value = row[column_number]
            if value is None:
                continue
            tree.insert(value, rid)

    def _iterate_existing_rows(self) -> Iterable[Tuple[int, List[Optional[int]]]]:
        if hasattr(self.table, "iter_rows_for_index"):
            yield from self.table.iter_rows_for_index()
            return

        if hasattr(self.table, "page_directory") and hasattr(self.table, "get_record_by_rid"):
            for rid in self.table.page_directory.keys():
                record = self.table.get_record_by_rid(rid)
                if record is None:
                    continue
                yield rid, record.columns'''
