from collections import defaultdict
from time import time

from config import Config
from lstore.index import Index
from lstore.page import Page


class Record:
    """Lightweight holder for a single row's column values."""

    def __init__(self, key, columns, is_tail=False):
        self.rid = None
        self.key = key
        self.columns = columns
        self.is_tail = is_tail

    def __getitem__(self, column):
        return self.columns[column]

    def __setitem__(self, column, value):
        self.columns[column] = value

    def __str__(self):
        return f"{self.rid}, {self.key}, {self.columns}"

    def __repr__(self):
        return f"Record(rid={self.rid}, key={self.key}, columns={self.columns}, is_tail={self.is_tail})"

class PageDirectory:
    def __init__(self, num_columns: int, num_ranges: int = Config.initial_page_ranges):
        # Each range lazily maps to base and tail logical pages (column-major Page instances).
        self.page_directory = defaultdict(lambda: {"base": [], "tail": []})
        self.num_columns = num_columns
        self.num_ranges = num_ranges
        self.num_base_records = 0
        self.num_tail_records = 0
        self.base_offsets = defaultdict(int)
        self.tail_offsets = defaultdict(int)


    def encode_rid(self, range_id, segment, offset):
        return range_id * Config.range_cap + segment * Config.records_per_range + offset

    def decode_rid(self, rid: int):
        range_id = rid // Config.range_cap
        segment_block = rid % Config.range_cap
        segment = segment_block // Config.records_per_range
        offset = segment_block % Config.records_per_range
        page_index = offset // Config.records_per_page
        slot_index = offset % Config.records_per_page
        return range_id, segment, page_index, slot_index


    def add_record(self, columns: list[int], is_tail: bool = False, base_rid: int = Config.null_value):
        """
        Adds a record to the page directory
        :param columns: list[int] - the columns of the record, includes meta columns
        :param is_tail: bool - whether the record is a tail record
        :param base_rid: int - the RID of the base record, only used for tail records
        """
        expected_len = (Config.tail_meta_columns if is_tail else Config.base_meta_columns) + self.num_columns
        num_columns = len(columns)
        if num_columns != expected_len:
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
            base_range = self.decode_rid(base_rid)[0]
            offset = self.tail_offsets[base_range]
            if offset >= Config.records_per_range:
                raise RuntimeError("Tail range is full; merge required before inserting more tail records")
            rid = self.encode_rid(base_range, 1, offset)
            self.tail_offsets[base_range] += 1
            columns[Config.schema_encoding_column] = self.build_schema_encoding(columns)
            base_record = self.get_record_from_rid(base_rid)
            if base_record[Config.indirection_column] != Config.null_value:
                columns[Config.indirection_column] = base_record[Config.indirection_column]
            else:
                columns[Config.indirection_column] = base_rid
            columns[Config.base_rid_column] = base_rid

        columns[Config.timestamp_column] = int(time())
        range_id, _, page_index, _ = self.decode_rid(rid)
        segment_key = "tail" if is_tail else "base"
        
        while page_index >= len(self.page_directory[range_id][segment_key]):
            # Each logical page holds one columnar Page per column (meta + data).
            self.page_directory[range_id][segment_key].append(
                [Page() for _ in range(num_columns)]
            )
        

        columns[Config.rid_column] = rid
        for i, value in enumerate(columns):
            physical_page = self.page_directory[range_id][segment_key][page_index][i]
            physical_page.write(value)
        
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
        segment_key = "base"
        base_indirection_page = self.page_directory[range_id][segment_key][page_index][Config.indirection_column]
        base_indirection_page.write_slot(slot_index, tail_columns[Config.rid_column])
        base_schema_encoding_page = self.page_directory[range_id][segment_key][page_index][Config.schema_encoding_column]
        base_schema_encoding = base_schema_encoding_page.read(slot_index)
        new_schema_encoding = self.build_schema_encoding(tail_columns)
        base_schema_encoding_page.write_slot(slot_index, new_schema_encoding | base_schema_encoding)
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
        segment_key = "tail" if segment else "base"
        num_columns = (Config.tail_meta_columns if segment else Config.base_meta_columns) + self.num_columns
        logical_page = self.page_directory[range_id][segment_key][page_index]
        columns = [logical_page[i].read(slot_index) for i in range(num_columns)]

        if not segment and columns[Config.indirection_column] == Config.deleted_record_value:
            raise RuntimeError(f"Record with RID {rid} has been deleted")

        return columns


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
        while current_rid != Config.null_value:
            tail_record = self.get_record_from_rid(current_rid)
            tails_newest_first.append(tail_record)
            current_rid = tail_record[Config.indirection_column]

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
            return False

        range_id = rid // Config.records_per_range
        page_index = (rid // Config.records_per_page) % Config.pages_per_range
        if range_id not in self.page_directory or not self.page_directory[range_id]["base"]:
            return False
        if page_index >= len(self.page_directory[range_id]["base"]):
            return False

        slot_index = rid % Config.records_per_page
        rid_page = self.page_directory[range_id]["base"][page_index][Config.rid_column]
        indirection_page = self.page_directory[range_id]["base"][page_index][Config.indirection_column]

        current_rid = rid_page.read(slot_index)
        if current_rid == Config.null_value:
            return True

        indirection_page.write_slot(slot_index, Config.deleted_record_value)
        return True

class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = PageDirectory(num_columns, Config.initial_page_ranges)
        self.index = Index(self)
        pass

    def get_record(self, rid: int):
        """
        Gets a record from the table
        :param rid: int - the RID of the record
        :return: list[int] - the columns of the record
        """
        return self.page_directory.get_record_from_rid(rid)

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
        return self.page_directory.add_record(columns, is_tail=is_tail, base_rid=base_rid)

    def delete_record(self, rid: int):
        """
        Deletes a record from the table
        :param rid: int - the RID of the base record
        :return: bool - whether the record was deleted
        """
        return self.page_directory.delete_record(rid)

    def __merge(self):
        print("merge is happening")
        pass
 
