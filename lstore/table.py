from lstore.index import Index
from time import time
from config import Config
from collections import defaultdict
from lstore.page import Page

class Record:
    """
    Not sure how to use this yet
    """

    def __init__(self, rid, key, columns, is_base=True):
        self.rid = rid
        self.key = key
        self.columns = columns

    def __getitem__(self, column):
        return self.columns[column]

    def __str__(self):
        return f"{self.rid}, {self.key}, {self.columns}"

    def __repr__(self):
        return "Record(rid={self.rid}, key={self.key}, columns={self.columns}, is_base={self.is_base})"

class PageDirectory:
    def __init__(self, num_columns: int, num_ranges: int = Config.initial_page_ranges):
        self.page_directory = defaultdict(dict) # key: page_range, value: list of pages
        self.num_columns = num_columns
        self.num_ranges = num_ranges
        self.records_per_range = Config.records_per_page * Config.pages_per_range
        for range_id in range(num_ranges):
            self.page_directory[range_id] = {
                "base": [Page() for _ in range(self.num_columns)],
                "tail": [Page() for _ in range(self.num_columns)]
            }

    def add_record(self, columns: list[int]):
        # Need to check if num_ranges is reached
        range_id = columns[Config.rid_column] // self.records_per_range
        
        self.page_directory[range_id]["base"]

    def get_record_from_rid(self, rid: int):
        page_range = rid % self.num_ranges
        pages = self.page_directory[page_range]
        for page in pages:  
            if page.has_record(rid):
                return page.get_record(rid)
        return None


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
        self.page_directory = PageDirectory(Config.initial_page_ranges)
        self.index = Index(self)
        pass

    def get_record(self, rid: int):
        page_range = rid % Config.initial_page_ranges
        pages = self.page_directory.get_page_range(page_range)

        return None

    def __merge(self):
        print("merge is happening")
        pass
 
