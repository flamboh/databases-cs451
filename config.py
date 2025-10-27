"""
Centralized configuration for the database
"""
class Config:
    page_size = 4096 # bytes
    int_size = 8
    records_per_page = page_size // int_size
    pages_per_range = 16
    records_per_range = records_per_page * pages_per_range
    range_cap = records_per_range * 2
    byteorder = 'little'
    indirection_column = 0
    rid_column = 1
    timestamp_column = 2
    schema_encoding_column = 3
    base_rid_column = 4 # for tail records
    initial_page_ranges = 1
    base_meta_columns = 4
    tail_meta_columns = 5
    null_value = -2**63
    deleted_record_value = -1