from lstore.bplus import BPlusTree

"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] * table.num_columns
        self.table = table
        self.indices[table.key] = BPlusTree(order=32)

    def insert(self, rid, key_value, column=None):
        col = self.table.key if column is None else column
        tree = self.indices[col]
        if tree is not None:
            tree.insert(key_value, rid)

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        tree = self.indices[column]
        if tree is None:
            return []
        return tree.find(value)

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        tree = self.indices[column]
        if tree is None:
            return []
        return tree.find_range(begin, end)

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        if self.indices[column_number] is None:
            self.indices[column_number] = BPlusTree(order=32)

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None

    """
    # returns the RID if key exists
    """
    def lookup(self, key_value):
        col_index = self.table.key
        tree = self.indices[col_index]
        if tree is None:
            return None
        rids = tree.find(key_value)
        return rids[0] if rids else None
