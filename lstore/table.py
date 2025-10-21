from lstore.index import Index
from time import time
from config import Config

class Record:

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
        self.page_directory = {}
        self.index = Index(self)
        pass

    def __merge(self):
        print("merge is happening")
        pass
 
