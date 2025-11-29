from collections import defaultdict
from contextlib import ExitStack, contextmanager
from time import time
from typing import Optional
import threading

from config import Config
from lstore.bufferpool import Bufferpool
from lstore.index import Index


class Record:
    """Lightweight holder for a single row's column values."""

    def __init__(self, key, columns):
        self.rid = None
        self.key = key
        self.columns = columns

    def __getitem__(self, column):
        return self.columns[column]

    def __setitem__(self, column, value):
        self.columns[column] = value

    def __str__(self):
        return f"{self.rid}, {self.key}, {self.columns}"

    def __repr__(self):
        return f"Record(rid={self.rid}, key={self.key}, columns={self.columns})"


class ColumnPageProxy:
    """
    Proxy object that exposes the Page API while routing all reads/writes through the bufferpool.
    This keeps tests that inspect Page objects working without materializing full in-memory copies.
    """

    __slots__ = ("bufferpool", "table_name", "range_id", "segment", "page_index", "column_index")

    def __init__(self, bufferpool, table_name, range_id, segment, page_index, column_index):
        self.bufferpool = bufferpool
        self.table_name = table_name
        self.range_id = range_id
        self.segment = segment
        self.page_index = page_index
        self.column_index = column_index

    def _open_page(self):
        return self.bufferpool.page(
            self.table_name,
            self.range_id,
            self.segment,
            self.page_index,
            self.column_index,
        )

    def write(self, value):
        with self._open_page() as page:
            return page.write(value)

    def write_slot(self, slot_index, value):
        with self._open_page() as page:
            return page.write_slot(slot_index, value)

    def read(self, slot_index):
        with self._open_page() as page:
            return page.read(slot_index)

    def read_range(self, start=0, end=None):
        with self._open_page() as page:
            return page.read_range(start=start, end=end)

    @property
    def num_records(self):
        with self._open_page() as page:
            return page.num_records

    def has_capacity(self):
        with self._open_page() as page:
            return page.has_capacity()

    def __repr__(self):
        return (
            f"ColumnPageProxy(table={self.table_name}, range={self.range_id}, "
            f"segment={self.segment}, page={self.page_index}, col={self.column_index})"
        )

class PageDirectory:
    def __init__(
        self,
        table_name: str,
        num_columns: int,
        bufferpool: Bufferpool,
        num_ranges: int = Config.initial_page_ranges,
        metadata: Optional[dict] = None,
    ):
        self.table_name = table_name
        self.bufferpool = bufferpool
        self.num_columns = num_columns
        self.num_ranges = num_ranges
        self.page_directory = defaultdict(self._new_range_entry)
        self.num_base_records = 0
        self.num_tail_records = 0
        self.base_offsets = defaultdict(int)
        self.tail_offsets = defaultdict(int)
        self.tail_merge_progress = defaultdict(int)
        self.merge_thresholds = defaultdict(lambda: max(1, Config.records_per_range // 4))

        if metadata:
            self._load_metadata(metadata)

    def _new_range_entry(self):
        return {"base": [], "tail": [], "TPS": 0}

    # ------------------------------------------------------------------
    # RID helpers
    # ------------------------------------------------------------------
    def encode_rid(self, range_id, segment, offset):
        segments_per_range = Config.segments_per_range
        global_segment = range_id * segments_per_range + segment
        return global_segment * Config.records_per_range + offset

    def decode_rid(self, rid: int):
        segment_span = Config.records_per_range
        global_segment = rid // segment_span
        offset = rid % segment_span
        segments_per_range = Config.segments_per_range
        segment = global_segment % segments_per_range
        range_id = global_segment // segments_per_range
        page_index = offset // Config.records_per_page
        slot_index = offset % Config.records_per_page
        return range_id, segment, page_index, slot_index

    # ------------------------------------------------------------------
    # Page bookkeeping helpers
    # ------------------------------------------------------------------
    def _get_or_create_range(self, range_id: int) -> dict:
        info = self.page_directory[range_id]
        self.num_ranges = max(self.num_ranges, range_id + 1)
        return info

    def _require_range(self, range_id: int) -> dict:
        if range_id not in self.page_directory:
            raise RuntimeError(f"Range {range_id} has no allocated pages")
        return self.page_directory[range_id]

    def _ensure_logical_page(self, range_id: int, segment: int, page_index: int):
        info = self._get_or_create_range(range_id)
        if segment == 0:
            pages = info["base"]
            column_count = self._column_count(is_tail=False)
        else:
            tail_idx = segment - 1
            tail_segments = info["tail"]
            while len(tail_segments) <= tail_idx:
                tail_segments.append([])
            pages = tail_segments[tail_idx]
            column_count = self._column_count(is_tail=True)

        while len(pages) <= page_index:
            new_page_index = len(pages)
            segment_label = self._segment_label(segment)
            logical_page = [
                ColumnPageProxy(
                    self.bufferpool,
                    self.table_name,
                    range_id,
                    segment_label,
                    new_page_index,
                    column_index,
                )
                for column_index in range(column_count)
            ]
            pages.append(logical_page)

        return pages[page_index]

    def _ensure_page_exists_for_read(self, range_id: int, segment: int, page_index: int) -> None:
        info = self._require_range(range_id)
        if segment == 0:
            if page_index >= len(info["base"]):
                raise RuntimeError(
                    f"Base page {page_index} missing for range {range_id}"
                )
        else:
            tail_idx = segment - 1
            if tail_idx >= len(info["tail"]):
                raise RuntimeError(
                    f"Tail segment {segment} missing for range {range_id}"
                )
            if page_index >= len(info["tail"][tail_idx]):
                raise RuntimeError(
                    f"Tail page {page_index} missing for range {range_id}, segment {segment}"
                )

    def _segment_label(self, segment_id: int) -> str:
        return "base" if segment_id == 0 else f"tail_{segment_id}"

    def _column_count(self, is_tail: bool) -> int:
        meta_columns = Config.tail_meta_columns if is_tail else Config.base_meta_columns
        return meta_columns + self.num_columns

    @contextmanager
    def _column(self, range_id: int, segment: int, page_index: int, column_index: int):
        label = self._segment_label(segment)
        with self.bufferpool.page(
            self.table_name, range_id, label, page_index, column_index
        ) as page:
            yield page

    @contextmanager
    def _logical_page(self, range_id: int, segment: int, page_index: int):
        logical_page = self._ensure_logical_page(range_id, segment, page_index)
        label = self._segment_label(segment)
        with ExitStack() as stack:
            pages = [
                stack.enter_context(
                    self.bufferpool.page(
                        self.table_name,
                        range_id,
                        label,
                        page_index,
                        proxy.column_index,
                    )
                )
                for proxy in logical_page
            ]
            yield pages

    # ------------------------------------------------------------------
    # Metadata serialization helpers
    # ------------------------------------------------------------------
    def to_metadata(self) -> dict:
        def stringify(mapping: defaultdict) -> dict:
            return {str(k): v for k, v in mapping.items()}

        directory = {}
        for range_id, info in self.page_directory.items():
            directory[str(range_id)] = {
                "base_pages": len(info["base"]),
                "tail_pages": [len(segment) for segment in info["tail"]],
                "TPS": info["TPS"],
            }
        return {
            "num_ranges": self.num_ranges,
            "num_base_records": self.num_base_records,
            "num_tail_records": self.num_tail_records,
            "base_offsets": stringify(self.base_offsets),
            "tail_offsets": stringify(self.tail_offsets),
            "tail_merge_progress": stringify(self.tail_merge_progress),
            "merge_thresholds": stringify(self.merge_thresholds),
            "page_directory": directory,
        }

    def _load_metadata(self, metadata: dict) -> None:
        self.num_ranges = metadata.get("num_ranges", self.num_ranges)
        self.num_base_records = metadata.get("num_base_records", 0)
        self.num_tail_records = metadata.get("num_tail_records", 0)

        def int_keys(values: dict) -> dict:
            return {int(k): v for k, v in values.items()}

        self.base_offsets = defaultdict(int, int_keys(metadata.get("base_offsets", {})))
        self.tail_offsets = defaultdict(int, int_keys(metadata.get("tail_offsets", {})))
        self.tail_merge_progress = defaultdict(
            int, int_keys(metadata.get("tail_merge_progress", {}))
        )
        self.merge_thresholds = defaultdict(
            lambda: max(1, Config.records_per_range // 4),
            int_keys(metadata.get("merge_thresholds", {})),
        )

        self.page_directory = defaultdict(self._new_range_entry)
        for range_id_str, info in metadata.get("page_directory", {}).items():
            range_id = int(range_id_str)
            range_entry = self._get_or_create_range(range_id)
            range_entry["TPS"] = info.get("TPS", 0)
            base_pages = info.get("base_pages", 0)
            for page_index in range(base_pages):
                self._ensure_logical_page(range_id, 0, page_index)
            for segment_offset, page_count in enumerate(info.get("tail_pages", []), start=1):
                for page_index in range(page_count):
                    self._ensure_logical_page(range_id, segment_offset, page_index)


    def peek_base_rid(self):
        """
        Compute the next base RID without mutating offsets.
        Callers must hold the same critical section used for add_record to avoid mismatch.
        """
        range_id = self.num_base_records // Config.records_per_range
        offset = self.base_offsets[range_id]
        return self.encode_rid(range_id, 0, offset)


    def add_record(
        self,
        columns: list[int],
        is_tail: bool = False,
        base_rid: int = Config.null_value,
    ):
        """
        Adds a record to the page directory
        :param columns: list[int] - the columns of the record, includes meta columns
        :param is_tail: bool - whether the record is a tail record
        :param base_rid: int - the RID of the base record, only used for tail records
        """
        expected_len = self._column_count(is_tail)
        if len(columns) != expected_len:
            raise ValueError(
                "Expected {expected} columns ({kind} meta columns + {data} data columns), got {actual}".format(
                    expected=expected_len,
                    kind="tail" if is_tail else "base",
                    data=self.num_columns,
                    actual=len(columns),
                )
            )

        if not is_tail:
            range_id = self.num_base_records // Config.records_per_range
            offset = self.base_offsets[range_id]
            rid = self.encode_rid(range_id, 0, offset)
            self.base_offsets[range_id] += 1
            columns[Config.indirection_column] = Config.null_value
            columns[Config.schema_encoding_column] = 0
        else:
            if base_rid == Config.null_value:
                raise ValueError("Tail records require a valid base RID")
            base_range = self.decode_rid(base_rid)[0]
            total_offset = self.tail_offsets[base_range]
            pending_updates = total_offset - self.tail_merge_progress[base_range]
            if pending_updates >= self.merge_thresholds[base_range]:
                self.merge_range(base_range)
                self.merge_thresholds[base_range] = max(1, self.merge_thresholds[base_range] * 2)
                total_offset = self.tail_offsets[base_range]

            max_capacity = Config.records_per_range * Config.max_tail_segments
            if total_offset >= max_capacity:
                raise RuntimeError("Tail range is full; merge required before inserting more tail records")

            segment_index = total_offset // Config.records_per_range
            segment_offset = total_offset % Config.records_per_range
            segment_id = 1 + segment_index

            rid = self.encode_rid(base_range, segment_id, segment_offset)
            self.tail_offsets[base_range] += 1
            columns[Config.schema_encoding_column] = self.build_schema_encoding(columns)
            base_record = self.get_record_from_rid(base_rid)
            base_indirection = base_record[Config.indirection_column]
            columns[Config.indirection_column] = (
                base_indirection if base_indirection != Config.null_value else base_rid
            )
            columns[Config.base_rid_column] = base_rid

        columns[Config.timestamp_column] = int(time())
        range_id, segment_id, page_index, _ = self.decode_rid(rid)
        self._ensure_logical_page(range_id, segment_id, page_index)

        columns[Config.rid_column] = rid
        with self._logical_page(range_id, segment_id, page_index) as column_pages:
            for column_index, value in enumerate(columns):
                if column_pages[column_index].write(value) is False:
                    raise RuntimeError(
                        f"Failed to append value to page (range={range_id}, segment={segment_id}, page={page_index})"
                    )

        if not is_tail:
            self.num_base_records += 1
        else:
            self.num_tail_records += 1

        if is_tail and base_rid != Config.null_value:
            self.update_base_record(base_rid, columns)

        return rid

    def update_base_record(self, base_rid: int, tail_columns: list[int]):
        """
        Updates a base record
        :param base_rid: int - the RID of the base record
        :param tail_columns: list[int] - the columns of the latest tail record
        :return: bool - whether the record was updated
        """
        range_id, _, page_index, slot_index = self.decode_rid(base_rid)
        with self._column(range_id, 0, page_index, Config.indirection_column) as indirection_page:
            indirection_page.write_slot(slot_index, tail_columns[Config.rid_column])
        with self._column(range_id, 0, page_index, Config.schema_encoding_column) as schema_page:
            base_schema_encoding = schema_page.read(slot_index)
            new_schema_encoding = self.build_schema_encoding(tail_columns)
            schema_page.write_slot(slot_index, new_schema_encoding | base_schema_encoding)
        return True


    def build_schema_encoding(self, tail_columns: list[int]):
        """
        Builds a schema encoding from a list of columns
        :param columns: list[int] - the columns of the record
        :return: int - the schema encoding
        """
        schema_encoding = 0
        num_data_columns = len(tail_columns) - Config.tail_meta_columns
        for i in range(num_data_columns):
            if tail_columns[i + Config.tail_meta_columns] != Config.null_value:
                schema_encoding |= 1 << (num_data_columns - i - 1)
        return schema_encoding


    def get_record_from_rid(self, rid: int):
        """
        Gets a record from the table
        :param rid: int - the RID of the record
        :param is_tail: bool - whether the record is a tail record
        :return: list[int] - the columns of the record
        """
        range_id, segment, page_index, slot_index = self.decode_rid(rid)
        self._ensure_page_exists_for_read(range_id, segment, page_index)
        with self._logical_page(range_id, segment, page_index) as column_pages:
            return [column_pages[i].read(slot_index) for i in range(len(column_pages))]


    def get_relative_version_of_record_from_base_rid(self, base_rid: int, version: int = -1):
        """
        Gets a cumulative updated version of a record from the table, defaults to latest (-1), 0 for base record
        :param base_rid: int - the RID of the base record
        :param version: int - the relative version of the record, decrease to get older versions, defaults to latest
        :return: list[int] - the columns of the record, with a tail record
        """
        base_record = self.get_record_from_rid(base_rid)
        result_record = (
            base_record[: Config.base_meta_columns]
            + [base_record[Config.rid_column]]
            + base_record[Config.base_meta_columns :]
        )

        if version == 0:
            return result_record

        if version == -1:
            return self.get_cumulative_updated_record_from_base_rid(base_rid)

        # Gather the tail chain so we can walk versions from oldest to newest.
        tails_newest_first = []
        current_rid = base_record[Config.indirection_column]
        if current_rid in (Config.null_value, Config.deleted_record_value):
            current_rid = base_rid

        while current_rid != base_rid:
            tail_record = self.get_record_from_rid(current_rid)
            tails_newest_first.append(tail_record)
            current_rid = tail_record[Config.indirection_column]
            if current_rid == Config.null_value:
                current_rid = base_rid

        tails_oldest_first = tails_newest_first[::-1]
        data_column_count = self.num_columns + Config.tail_meta_columns
        # version = -2 means "latest minus one", etc.
        if version < -1:
            skip_newest = (-1 - version)
            apply_count = max(0, len(tails_oldest_first) - skip_newest)
        else:
            apply_count = min(version, len(tails_oldest_first))

        for tail_record in tails_oldest_first[:apply_count]:
            for column_index in range(Config.tail_meta_columns, data_column_count):
                if tail_record[column_index] != Config.null_value:
                    result_record[column_index] = tail_record[column_index]

        return result_record

    def get_cumulative_updated_record_from_base_rid(self, base_rid: int):
        """
        Gets a cumulative updated record from the table
        :param base_rid: int - the RID of the base record
        :return: list[int] - the columns of the record
        """
        base_record = self.get_record_from_rid(base_rid)
        result_record = (
            base_record[: Config.base_meta_columns]
            + [base_record[Config.rid_column]]
            + base_record[Config.base_meta_columns :]
        )

        data_column_count = self.num_columns + Config.tail_meta_columns

        indirection_rid = base_record[Config.indirection_column]
        if indirection_rid == Config.null_value:
            return result_record
        schema_encoding = base_record[Config.schema_encoding_column]
        while schema_encoding != 0 and indirection_rid != base_rid:
            current_record = self.get_record_from_rid(indirection_rid)
            indirection_rid = current_record[Config.indirection_column]
            for column_index in range(Config.tail_meta_columns, data_column_count):
                bit_mask = 1 << (data_column_count - column_index - 1)
                if schema_encoding & bit_mask and current_record[column_index] != Config.null_value:
                    result_record[column_index] = current_record[column_index]
                    schema_encoding &= ~bit_mask
        return result_record

    def delete_record(self, rid: int):
        """
        Logical deletion of a record from the table
        :param rid: int - the RID of the record
        :return: bool - whether the record was deleted
        """
        if rid < 0 or rid >= self.num_base_records:
            raise ValueError(f"Invalid RID: {rid}")

        range_id, segment, page_index, slot_index = self.decode_rid(rid)
        if segment:
            raise ValueError("A tail record RID cannot be deleted")

        self._ensure_page_exists_for_read(range_id, 0, page_index)
        with ExitStack() as stack:
            rid_page = stack.enter_context(
                self._column(range_id, 0, page_index, Config.rid_column)
            )
            indirection_page = stack.enter_context(
                self._column(range_id, 0, page_index, Config.indirection_column)
            )
            try:
                current_rid = rid_page.read(slot_index)
                indirection_value = indirection_page.read(slot_index)
            except IndexError as exc:
                raise ValueError(f"RID {rid} resolved to an invalid slot index") from exc

            if current_rid == Config.null_value:
                raise ValueError(f"RID {rid} refers to an empty slot")
            if indirection_value == Config.deleted_record_value:
                return False

            indirection_page.write_slot(slot_index, Config.deleted_record_value)
        return True

    def merge_range(self, range_id):
        range_info = self.page_directory.get(range_id)
        if not range_info:
            return False
        tail_segments = range_info["tail"]
        start_offset = self.tail_merge_progress[range_id]
        end_offset = self.tail_offsets[range_id]
        if start_offset >= end_offset:
            return False

        merged_any = False
        for segment_index, segment_pages in enumerate(tail_segments, start=1):
            for page_index, _ in enumerate(segment_pages):
                with self._logical_page(range_id, segment_index, page_index) as tail_pages:
                    if not tail_pages:
                        continue
                    num_records = tail_pages[Config.rid_column].num_records
                    for tail_slot in range(num_records):
                        tail_columns = [column_page.read(tail_slot) for column_page in tail_pages]
                        tail_rid = tail_columns[Config.rid_column]
                        base_rid = tail_columns[Config.base_rid_column]
                        if base_rid == Config.null_value:
                            continue

                        _, tail_segment, tail_page_index, tail_slot_index = self.decode_rid(tail_rid)
                        tail_segment_index = max(0, tail_segment - 1)
                        tail_offset = (
                            tail_segment_index * Config.records_per_range
                            + tail_page_index * Config.records_per_page
                            + tail_slot_index
                        )
                        if tail_offset < start_offset or tail_offset >= end_offset:
                            continue

                        base_range, _, base_page_index, base_slot_index = self.decode_rid(base_rid)
                        self._ensure_page_exists_for_read(base_range, 0, base_page_index)
                        with self._logical_page(base_range, 0, base_page_index) as base_pages:
                            for column_index in range(self.num_columns):
                                value = tail_columns[Config.tail_meta_columns + column_index]
                                if value == Config.null_value:
                                    continue
                                base_column_page = base_pages[Config.base_meta_columns + column_index]
                                base_column_page.write_slot(base_slot_index, value)

                            base_schema_page = base_pages[Config.schema_encoding_column]
                            base_schema_page.write_slot(base_slot_index, 0)
                        merged_any = True

        if not merged_any:
            return False

        self.tail_merge_progress[range_id] = end_offset
        if end_offset == 0:
            tps_value = 0
        else:
            last_offset = end_offset - 1
            last_segment_index = last_offset // Config.records_per_range
            last_segment_id = 1 + last_segment_index
            last_segment_offset = last_offset % Config.records_per_range
            tps_value = self.encode_rid(range_id, last_segment_id, last_segment_offset)
        range_info["TPS"] = tps_value
        return True


class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(
        self,
        name,
        num_columns,
        key,
        bufferpool: Optional[Bufferpool] = None,
        directory_metadata: Optional[dict] = None,
    ):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.bufferpool = bufferpool or Bufferpool()
        self.page_directory = PageDirectory(
            name,
            num_columns,
            self.bufferpool,
            Config.initial_page_ranges,
            metadata=directory_metadata,
        )
        self.index = Index(self)
        self._insert_lock = threading.RLock()


    def to_metadata(self) -> dict:
        return {
            "num_columns": self.num_columns,
            "key": self.key,
            "directory": self.page_directory.to_metadata(),
        }

    def get_record(self, rid: int):
        """
        Gets a record from the table
        :param rid: int - the RID of the record
        :return: list[int] - the columns of the record
        """
        return self.page_directory.get_record_from_rid(rid)

    def iter_rows_for_index(self):
        """
        Yields (rid, data_columns) for every non-deleted base record.
        Used by Index to build secondary structures without peeking into internals.
        """
        directory = self.page_directory
        if not directory.base_offsets:
            return

        # Iterate ranges deterministically so index construction is reproducible.
        for range_id in sorted(directory.base_offsets.keys()):
            record_count = directory.base_offsets[range_id]
            if record_count <= 0:
                continue
            for offset in range(record_count):
                rid = directory.encode_rid(range_id, 0, offset)
                try:
                    record = directory.get_cumulative_updated_record_from_base_rid(rid)
                except RuntimeError:
                    continue
                if record[Config.indirection_column] == Config.deleted_record_value:
                    continue
                yield rid, record[Config.tail_meta_columns : Config.tail_meta_columns + self.num_columns]

    def get_relative_version_of_record(self, rid: int, version: int = -1):
        """
        Gets a relative version of a record from the table
        :param rid: int - the RID of the record
        :param version: int - the relative version of the record, increase to get older versions, defaults to latest
        :return: list[int] - the columns of the record
        """
        return self.page_directory.get_relative_version_of_record_from_base_rid(rid, version)

    def get_cumulative_updated_record(self, rid: int):
        """
        Gets an updated record from the table
        :param rid: int - the RID of the record
        :return: list[int] - the columns of the record
        """
        return self.page_directory.get_cumulative_updated_record_from_base_rid(rid)

    def insert_record(self, columns: list[int], is_tail: bool = False, base_rid: int = Config.null_value):
        """
        Inserts a record into the table
        :param columns: list[int] - the columns of the record
        :param base_rid: int - the RID of the base record, only used for tail records
        :return: int - the RID of the record
        """
        with self._insert_lock:
            return self._insert_record_locked(columns, is_tail=is_tail, base_rid=base_rid)

    def _insert_record_locked(self, columns: list[int], is_tail: bool = False, base_rid: int = Config.null_value):
        """
        Internal helper that assumes _insert_lock is held.
        """
        prior_data = None
        if is_tail:
            prior_data = self.get_cumulative_updated_record(base_rid)[
                Config.tail_meta_columns : Config.tail_meta_columns + self.num_columns
            ]

        rid = self.page_directory.add_record(columns, is_tail=is_tail, base_rid=base_rid)

        if not is_tail:
            base_data = columns[
                Config.base_meta_columns : Config.base_meta_columns + self.num_columns
            ]
            self.index.add(rid, base_data)
        else:
            updated_data = self.get_cumulative_updated_record(base_rid)[
                Config.tail_meta_columns : Config.tail_meta_columns + self.num_columns
            ]
            self.index.update(base_rid, prior_data, updated_data)
        return rid

    def delete_record(self, rid: int):
        """
        Deletes a record from the table
        :param rid: int - the RID of the base record
        :return: bool - whether the record was deleted
        """
        try:
            base_record = self.page_directory.get_record_from_rid(rid)
        except RuntimeError:
            return False

        data_columns = base_record[Config.base_meta_columns : Config.base_meta_columns + self.num_columns]
        self.index.remove(rid, data_columns)

        try:
            return self.page_directory.delete_record(rid)
        except ValueError:
            return False

    
    def get_rid_for_lock(self):
        return self.page_directory.peek_base_rid()
