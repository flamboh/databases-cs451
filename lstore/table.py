from lstore.index import Index
from time import time
from config import Config
from collections import defaultdict
from lstore.page import Page

class Record:
    """
    implement later with table class methods
    """

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
        self.page_directory = defaultdict(lambda: {"base": [], "tail": []}) # key: page_range, value: dict of "base" and "tail" where each contains a list of logical pages (logical pages containg Page() objects)
        self.num_columns = num_columns
        self.num_ranges = num_ranges
        self.num_base_records = 0
        self.num_tail_records = 0
        self.base_offsets = defaultdict(int)
        self.tail_offsets = defaultdict(int)
        for range_id in range(num_ranges):
            self.page_directory[range_id] = {
                "base": [],
                "tail": []
            }

    def encode_rid(self, range_id, segment, offset):
        return range_id * Config.range_cap + segment * Config.records_per_range + offset
    
    def decode_rid(self, rid: int):
        range_id  = rid // Config.records_per_range
        seg_block = rid % Config.range_cap
        segment   = seg_block // Config.records_per_range
        offset    = seg_block % Config.records_per_range
        page_idx  = offset // Config.records_per_page
        slot_idx  = offset % Config.records_per_page
        return range_id, segment, page_idx, slot_idx


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
            raise ValueError(f"Expected {expected_len} columns ({"tail" if is_tail else "base"} meta columns + {self.num_columns} data columns), got {len(columns)}")
        # rid = self.num_base_records if not is_tail else self.num_tail_records

        if not is_tail:
            range_id = self.num_base_records // Config.records_per_range
            offset = self.base_offsets[range_id]
            rid = self.encode_rid(range_id, 0, offset)
            self.base_offsets[range_id] += 1
            columns[Config.schema_encoding_column] = 0
        else:
            base_range = self.decode_rid(base_rid)[0]
            offset = self.tail_offsets[base_range]
            rid = self.encode_rid(base_range, 1, offset)
            self.tail_offsets[base_range] += 1
            columns[Config.schema_encoding_column] = self.build_schema_encoding(columns)
            columns[Config.indirection_column] = self.get_version_of_record_from_base_rid(base_rid, -1)[Config.rid_column]
            columns[Config.base_rid_column] = base_rid

        columns[Config.timestamp_column] = int(time())
        range_id, _, page_index, _ = self.decode_rid(rid)
        segment_key = "tail" if is_tail else "base"
        
        if page_index >= len(self.page_directory[range_id][segment_key]):
            self.page_directory[range_id][segment_key].append([Page() for _ in range(num_columns)])
        
        if rid % Config.records_per_page == 0:
            self.page_directory[range_id][segment_key].append([Page() for _ in range(num_columns)])
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
        return columns


    def get_version_of_record_from_base_rid(self, base_rid: int, version: int = 0):
        """
        Gets a version of a record from the table, defaults to latest
        :param base_rid: int - the RID of the base record
        :param version: int - the relative version of the record, increase to get older versions, defaults to latest
        :return: list[int] - the columns of the record
        """
        base_record = self.get_record_from_rid(base_rid)
        if version == -1:
            return base_record
        i = 0
        current_record = base_record
        while i < version + 1:
            current_record = self.get_record_from_rid(current_record[Config.indirection_column])
            i += 1
        return current_record

    def get_updated_record_from_base_rid(self, base_rid: int):
        """
        Gets a cumulative updated record from the table
        :param base_rid: int - the RID of the base record
        :return: list[int] - the columns of the record
        """
        base_record = self.get_record_from_rid(base_rid)
        result_record = base_record.copy()
        
        
        num_columns = self.num_columns + Config.tail_meta_columns
        
        
        indirection_rid = base_record[Config.indirection_column]
        if indirection_rid == Config.null_value:
            return base_record

        current_record = result_record
        schema_encoding = base_record[Config.schema_encoding_column]
        while schema_encoding != 0 and indirection_rid != Config.null_value and current_record is not base_record :
            current_record = self.get_record_from_rid(indirection_rid)
            next_indirection = current_record[Config.indirection_column]
            updated_in_this_iter = 0
            for i in range(Config.tail_meta_columns, num_columns):
                bit = (1 << (num_columns - i - 1))
                if schema_encoding & bit:
                    if current_record[i] != Config.null_value:
                        result_record[i - 1] = current_record[i]
                        schema_encoding &= ~bit
                        updated_in_this_iter |= bit
            indirection_rid = next_indirection
        return result_record

    def delete_record(self, rid: int):
        """
        Logical deletion of a record from the table
        :param rid: int - the RID of the record
        :return: bool - whether the record was deleted
        """
        if rid < 0 or rid >= self.num_base_records:
            return False

        range_id = rid // Config.records_per_range # selects range
        page_index = (rid // Config.records_per_page) % Config.pages_per_range # select logical page
        if range_id not in self.page_directory or not self.page_directory[range_id]["base"]:
            return False
        if page_index >= len(self.page_directory[range_id]["base"]):
            return False

        slot_index = rid % Config.records_per_page # select slot
        current_rid = self.page_directory[range_id]["base"][page_index][Config.rid_column].read(slot_index)
        if current_rid == Config.null_value:
            return True
        self.page_directory[range_id]["base"][page_index][Config.rid_column].write_slot(slot_index, Config.null_value)
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

    def get_version_of_record(self, rid: int, version: int = 0):
        """
        Gets a version of a record from the table ## update to be cumulative
        :param rid: int - the RID of the record
        :param version: int - the relative version of the record, increase to get older versions, defaults to latest
        :return: list[int] - the columns of the record
        """
        return self.page_directory.get_version_of_record_from_base_rid(rid, version)

    def get_updated_record(self, rid: int):
        """
        Gets an updated record from the table
        :param rid: int - the RID of the record
        :return: list[int] - the columns of the record
        """
        return self.page_directory.get_updated_record_from_base_rid(rid)

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
 
