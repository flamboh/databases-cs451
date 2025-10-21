"""
Centralized configuration for the database
"""
class Config:
    page_size = 4096
    int_size = 8
    max_slots = page_size // int_size
    byteorder = 'little'
    indirection_column = 0
    rid_column = 1
    timestamp_column = 2
    schema_encoding_column = 3
