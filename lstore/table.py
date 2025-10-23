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
        self.is_base = is_base
        self.deleted = False

    def __getitem__(self, column):
        return self.columns[column]

    def __str__(self):
        return f"{self.rid}, {self.key}, {self.columns}"

    def __repr__(self):
        return f"Record(rid={self.rid}, key={self.key}, columns={self.columns}, is_base={self.is_base})"

class PageDirectory:
    def __init__(self, num_columns: int, num_ranges: int = Config.initial_page_ranges):
        self.page_directory = defaultdict(dict) # key: page_range, value: list of pages
        self.num_columns = num_columns
        self.num_ranges = num_ranges
        self.records_per_range = Config.records_per_page * Config.pages_per_range
        for range_id in range(num_ranges):
            self.page_directory[range_id] = {
                "base": [[Page()] for _ in range(self.num_columns)],
                "tail": [[Page()] for _ in range(self.num_columns)]
            }

    def add_record(self, rid: int, columns: list[int], is_base=True):
        range_id = rid // self.records_per_range
        target_type = "base" if is_base else "tail"
        
        for col_index in range(self.num_columns):
            pages = self.page_directory[range_id][target_type][col_index]
            for page in pages:
                if page.has_capacity():
                    page.write(columns[col_index])
                    break
                else:
                    new_page = Page(start_rid=rid)  # track new rid start
                    new_page.write(columns[col_index])
                    pages.append(new_page)



    def get_record_from_rid(self, rid: int):
        page_range = rid % self.num_ranges
        range_entry = self.page_directory[page_range]

        for page_type in ["base", "tail"]:
            for col_pages in range_entry[page_type]:
                for page in col_pages:
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
        self.page_directory = PageDirectory(self.num_columns)
        self.index = Index(self)
        self.next_rid = 0

    """
    # for primary key lookups
    """
    def rid(self, key):
        return self.index.lookup(key)
    
    def get_record(self, rid: int):
        """
        page_range = rid % Config.initial_page_ranges
        pages = self.page_directory.get_page_range(page_range)
        return None
        """
        return self.page_directory.get_record_from_rid(rid)

    def insert_record(self, columns: list[int], is_base=True):
        rid = self.next_rid
        self.next_rid += 1
        self.page_directory.add_record(rid, columns, is_base)
        self.index.insert(columns[self.key], rid)
        return rid


    def delete_record(self, rid: int):
        rec = self.get_record(rid)
        if rec is None:
            return False
        rec.deleted = True
        return True
    
    def update_record(self, rid: int, columns: list[int]):
        # get base
        old_record_values = self.get_record(rid)
        if old_record_values is None:
            return False
        
        # merge old and new
        updated_columns = [new if new is not None else old for new, old in zip(columns, old_record_values)]

        # insert into tail pages
        self.page_directory.add_record(rid, updated_columns, is_base=False)

        # update index if primary key changed
        key_column = self.key
        if updated_columns[key_column] != old_record_values[key_column]:
            self.index.insert(rid, updated_columns[key_column])

        return True
    
    def __merge(self):
        print("merge is happening")
        pass
