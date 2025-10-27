from lstore.table import Table, Record
from lstore.index import Index
from config import Config


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        try:
            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids:
                return False
            return all(self.table.delete_record(rid) for rid in rids)
        except Exception as e:
            print(e)
            return False  
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        try:
            if len(columns) != self.table.num_columns:
                print(f"[Insert] Column count mismatch: expected {self.table.num_columns}, got {len(columns)}")
                return False
            
            # construct full record (meta + data)
            full_record = [Config.null_value] * Config.base_meta_columns + list(columns)
            rid = self.table.insert_record(full_record, is_tail=False)

            # update index for primary key
            key = columns[self.table.key]
            self.table.index.insert(key, rid)
            return True
        except Exception as e:
            print(e)
            return False

    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        try:
            # locate RIDs via the index
            rids = self.table.index.locate(search_key_index, search_key)
            if not rids:
                return []

            results = []

            # for each RID, get the latest (merged) record data
            for rid in rids:
                full_record = self.table.get_cumulative_updated_record(rid)

                # strip metadata columns from start (RID, TID, etc.)
                data_start = Config.base_meta_columns
                data_end = data_start + self.table.num_columns
                data_columns = full_record[data_start:data_end]

                # apply the projection mask
                projected_data = [
                    data_columns[i] if projected_columns_index[i] == 1 else None
                    for i in range(self.table.num_columns)
                ]

                # build a Record object
                record = Record(search_key, projected_data)
                record.rid = rid
                results.append(record)

            return results
        except Exception as e:
            print(e)
            return False
    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        """
        Selects a specific historical version of records whose value in column
        `search_key_index` matches `search_key`.

        Arguments:
            search_key              – value to search for
            search_key_index        – index of the column to search in
            projected_columns_index – list of 0/1 flags for which columns to include
            relative_version        – 1 = most recent, 2 = previous, etc.

        Returns:
            A list of Record objects representing that version.
        """
        try:
            # find RIDs via the index
            rids = self.table.index.locate(search_key_index, search_key)
            if not rids:
                return []

            results = []

            # for each RID, get that version of the record
            for rid in rids:
                version_record = self.table.get_relative_version_of_record(rid, relative_version)
                if version_record is None:
                    continue  # no such version

                # strip metadata
                data_start = Config.base_meta_columns
                data_end = data_start + self.table.num_columns
                data_columns = version_record[data_start:data_end]

                # apply projection mask
                projected_data = [
                    data_columns[i] if projected_columns_index[i] == 1 else None
                    for i in range(self.table.num_columns)
                ]

                # build Record object
                record = Record(search_key, projected_data)
                record.rid = rid
                results.append(record)
            return results
        except Exception as e:
            print(e)
            return False
    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        try:
            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids:
                return False

            for rid in rids:
                base_record = self.table.get_cumulative_updated_record(rid)

                data_start = Config.base_meta_columns
                data_end = data_start + self.table.num_columns
                old_values = base_record[data_start:data_end]

                new_values = []
                for i in range(self.table.num_columns):
                    if i < len(columns) and columns[i] is not None:
                        new_values.append(columns[i])
                    else:
                        new_values.append(old_values[i])

                # construct tail record (meta + data)
                full_tail_record = [Config.null_value] * Config.tail_meta_columns + new_values
                self.table.insert_record(full_tail_record, is_tail=True, base_rid=rid)
            return True
        except Exception as e:
            print(e)
            return False

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        try:
            total = 0
            for key in range(start_range, end_range + 1):
                rids = self.table.index.locate(self.table.key, key)
                if not rids:
                    continue

                for rid in rids:
                    record_data = self.table.get_cumulative_updated_record(rid)
                    data_start = Config.base_meta_columns
                    data_end = data_start + self.table.num_columns
                    value = record_data[data_start:data_end][aggregate_column_index]
                    if value is not None:
                        total += value
            return total
        except Exception as e:
            print(e)
            return False
    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        try:
            total = 0
            for key in range(start_range, end_range + 1):
                rids = self.table.index.locate(self.table.key, key)
                if not rids:
                    continue

                for rid in rids:
                    record_data = self.table.get_relative_version_of_record(rid, relative_version)
                    data_start = Config.base_meta_columns
                    data_end = data_start + self.table.num_columns
                    value = record_data[data_start:data_end][aggregate_column_index]
                    if value is not None:
                        total += value
            return total
        except Exception as e:
            print(e)
            return False
    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        results = self.select(key, self.table.key, [1]*self.table.num_columns)
        if not results:
            return False
        r = results[0]
        updated_columns = [None] * self.table.num_columns
        updated_columns[column] = r[column] + 1
        return self.update(key, *updated_columns)
