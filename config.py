"""
Centralized configuration for the database
"""
class Config:
    page_size = 4096
    int_size = 8
    max_slots = page_size // int_size
    byteorder = 'little'