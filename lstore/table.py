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
        return "Record(rid={self.rid}, key={self.key}, columns={self.columns}, is_tail={self.is_tail})"

class PageDirectory:
    def __init__(self, num_columns: int, num_ranges: int = Config.initial_page_ranges):
        self.page_directory = defaultdict(dict) # key: page_range, value: dict of "base" and "tail" where each contains a list of logical pages (logical pages containg Page() objects)
        self.num_columns = num_columns
        self.num_ranges = num_ranges
        self.num_base_records = 0
        self.num_tail_records = 0
        self.records_per_range = Config.records_per_page * Config.pages_per_range
        for range_id in range(num_ranges):
            self.page_directory[range_id] = {
                "base": [],
                "tail": []
            }

    def add_record(self, columns: list[int], is_tail: bool = False):
        """
        Adds a record to the page directory
        :param columns: list[int] - the columns of the record
        :param is_tail: bool - whether the record is a tail record
        """
        # Need to check if num_ranges is reached
        rid = self.num_base_records if not is_tail else self.num_tail_records

        range_id = rid // self.records_per_range # selects range
        page_index = (rid // Config.records_per_page) % Config.pages_per_range # select logical page
        # slot_index = rid % Config.records_per_page # select slot
        print("adding record", rid, range_id, page_index)
        columns[Config.rid_column] = rid
        num_columns = len(columns)

        if range_id >= self.num_ranges:
            for i in range(range_id, self.num_ranges * 2):
                self.page_directory[i] = {
                    "base": [],
                    "tail": []
                }
            self.num_ranges *= 2
        
        if rid % Config.records_per_page == 0:
            self.page_directory[range_id][("tail" if is_tail else "base")].append([Page() for _ in range(num_columns)])
        for i, column in enumerate(columns):
            # print(self.page_directory[range_id][("tail" if is_tail else "base")])   
            self.page_directory[range_id][("tail" if is_tail else "base")][page_index][i].write(column)
        
        if not is_tail:
            self.num_base_records += 1
        else:
            self.num_tail_records += 1
        
    def get_base_record_from_rid(self, rid: int):
        range_id = rid // self.records_per_range # selects range
        page_index = (rid // Config.records_per_page) % Config.pages_per_range # select logical page
        slot_index = rid % Config.records_per_page # select slot
        print(rid, range_id, page_index, slot_index)
        num_columns = Config.base_meta_columns + self.num_columns
        columns = [self.page_directory[range_id]["base"][page_index][i].read(slot_index) for i in range(num_columns)]
        return columns


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
        return self.page_directory.get_base_record_from_rid(rid)

    def insert_record(self, columns: list[int]):
        """
        Inserts a record into the table
        :param columns: list[int] - the columns of the record
        """
        self.page_directory.add_record(columns)
        return True

    def __merge(self):
        print("merge is happening")
        pass
 
