"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        pass

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        #see if column has index
        if self.indices[column] is None:
            return []
        
        #see if value exists in index
        if value in self.indices[column]:
            return [self.indices[column][value]]
        else:
            return []

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        #see if column has index
        if self.indices[column] is None:
            return []

        result = []
        for value, rid in self.indices[column].items():
            if begin <= value <= end:
                result.append(rid)
        return result

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        if self.indices[column_number] is None:
            self.indices[column_number] = {}

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None
