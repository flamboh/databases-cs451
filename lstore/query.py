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
            # use index to find rid(s) for this primary key
            rids = self.table.index.locate(self.table.key, primary_key)
            
            if not rids:
                return False
            
            # delete first matching record (should only be one for primary key)
            rid = rids[0]
            result = self.table.delete_record(rid)
            
            return result
        except Exception:
            return False
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        try:
            # validate number of columns
            if len(columns) != self.table.num_columns:
                return False
            
            # check for None values - all columns must be non-NULL on insert
            if any(col is None for col in columns):
                return False
            
            # check if primary key already exists (only if index exists)
            primary_key = columns[self.table.key]
            if self.table.index.indices[self.table.key] is not None:
                existing_rids = self.table.index.locate(self.table.key, primary_key)
                if existing_rids:
                    return False
            
            # create base record with metadata columns
            base_meta = [Config.null_value for _ in range(Config.base_meta_columns)]
            record_data = base_meta + list(columns)
            
            # insert record (will also update the index via table.insert_record)
            rid = self.table.insert_record(record_data, is_tail=False)
            
            # manually add to index as a workaround to ensure proper indexing
            data_columns = list(columns)
            self.table.index.add(rid, data_columns)
            
            return rid is not False
        except Exception:
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
            # create index for search column if it doesn't exist (for non-primary key searches)
            if self.table.index.indices[search_key_index] is None:
                self.table.index.create_index(search_key_index)
            
            # use index to find matching RIDs
            rids = self.table.index.locate(search_key_index, search_key)
            
            if not rids:
                return []
            
            results = []
            for rid in rids:
                try:
                    # get cumulative updated record (follows indirection chain)
                    full_record = self.table.get_cumulative_updated_record(rid)
                    
                    # extract data columns (skip metadata columns)
                    # the cumulative record structure is:
                    # [indirection, rid, timestamp, schema_encoding, base_rid, ...data columns]
                    data_columns = full_record[Config.tail_meta_columns:]
                    
                    # get primary key from data columns
                    primary_key_value = data_columns[self.table.key]
                    
                    # apply projection
                    projected_data = []
                    for i, include in enumerate(projected_columns_index):
                        if include:
                            projected_data.append(data_columns[i])
                        else:
                            projected_data.append(None)
                    
                    # create record object
                    record = Record(primary_key_value, projected_data)
                    record.rid = rid
                    results.append(record)
                    
                except RuntimeError:
                    # record was deleted
                    continue
            
            return results
        except Exception:
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
        try:
            # create index for the search column if it doesn't exist (for non-primary key searches)
            if self.table.index.indices[search_key_index] is None:
                self.table.index.create_index(search_key_index)
            
            # use index to find matching rids
            rids = self.table.index.locate(search_key_index, search_key)
            
            if not rids:
                return []
            
            results = []
            for rid in rids:
                try:
                    # version interpretation:
                    # relative_version = 0 means the latest version
                    # relative_version = -1 means one version back from latest
                    # relative_version = -2 means two versions back from latest
                    # etc.
                    
                    # convert to internal version:
                    # internal version -1 means latest
                    # internal version -2 means one back
                    # internal version -3 means two back
                    internal_version = relative_version - 1
                    
                    # get the specific version of the record
                    try:
                        full_record = self.table.get_relative_version_of_record(rid, internal_version)
                    except IndexError:
                        # if we get an IndexError, it means we're trying to access a version
                        # that doesn't exist (e.g., asking for version -1 on a fresh insert).
                        # in this case, just return the base record (version 0)
                        full_record = self.table.get_relative_version_of_record(rid, 0)
                    
                    # extract data columns (skip metadata)
                    data_columns = full_record[Config.tail_meta_columns:]
                    
                    # get primary key from data columns
                    primary_key_value = data_columns[self.table.key]
                    
                    # apply projection
                    projected_data = []
                    for i, include in enumerate(projected_columns_index):
                        if include:
                            projected_data.append(data_columns[i])
                        else:
                            projected_data.append(None)
                    
                    # create Record object
                    record = Record(primary_key_value, projected_data)
                    record.rid = rid
                    results.append(record)
                    
                except RuntimeError:
                    # record was deleted
                    continue
            
            return results
        except Exception as e:
            print(f"select_version error: {e}")
            import traceback
            traceback.print_exc()
            return False

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        try:
            # validate number of columns
            if len(columns) != self.table.num_columns:
                return False
            
            # find record by primary key
            rids = self.table.index.locate(self.table.key, primary_key)
            
            if not rids:
                return False
            
            base_rid = rids[0]
            
            # get current record values to update index
            current_record = self.table.get_cumulative_updated_record(base_rid)
            current_data = current_record[Config.tail_meta_columns:]
            
            # create tail record with metadata
            tail_meta = [Config.null_value for _ in range(Config.tail_meta_columns)]
            tail_data = []
            
            # build new values and track what changed
            new_data = []
            for i, new_value in enumerate(columns):
                if new_value is None:
                    # no update for this column
                    tail_data.append(Config.null_value)
                    new_data.append(current_data[i])
                else:
                    # update this column
                    tail_data.append(new_value)
                    new_data.append(new_value)
            
            tail_record = tail_meta + tail_data
            
            # insert tail record
            tail_rid = self.table.insert_record(tail_record, is_tail=True, base_rid=base_rid)
            
            # update index for changed columns
            self.table.index.update(base_rid, current_data, new_data)
            
            return tail_rid is not False
        except Exception:
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
            # use index range query to find all rids in the key range
            rids = self.table.index.locate_range(start_range, end_range, self.table.key)
            
            if not rids:
                return 0
            
            total = 0
            found_any = False
            
            for rid in rids:
                try:
                    # get cumulative updated record
                    full_record = self.table.get_cumulative_updated_record(rid)
                    
                    # extract data columns
                    data_columns = full_record[Config.tail_meta_columns:]
                    
                    # add value from specified column
                    value = data_columns[aggregate_column_index]
                    if value is not None and value != Config.null_value:
                        total += value
                        found_any = True
                        
                except RuntimeError:
                    # record was deleted, skip it
                    continue
            
            return total if found_any else 0
        except Exception:
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
            # use index range query to find all rids in the key range
            rids = self.table.index.locate_range(start_range, end_range, self.table.key)
            
            if not rids:
                return 0
            
            total = 0
            found_any = False
            
            for rid in rids:
                try:
                    # convert version numbering from test convention to internal convention
                    # test convention: 0 = latest, -1 = one back, -2 = two back, etc.
                    # internal convention: -1 = latest, -2 = one back, -3 = two back, etc.
                    internal_version = relative_version - 1
                    
                    # get specific version of record
                    try:
                        full_record = self.table.get_relative_version_of_record(rid, internal_version)
                    except IndexError:
                        # if we get an IndexError, it means we're trying to access a version
                        # that doesn't exist. fall back to the base record.
                        full_record = self.table.get_relative_version_of_record(rid, 0)
                    
                    # extract data columns
                    data_columns = full_record[Config.tail_meta_columns:]
                    
                    # add value from specified column
                    value = data_columns[aggregate_column_index]
                    if value is not None and value != Config.null_value:
                        total += value
                        found_any = True
                        
                except RuntimeError:
                    # record was deleted, skip it
                    continue
            
            return total if found_any else 0
        except Exception:
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
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False