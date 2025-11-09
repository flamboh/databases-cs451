"""
*~ Bufferpool ~*

The bufferpool acts as a cache layer between in-memory page directory and disk storage...
...like RAM for database pages

Key Concepts:
    Fixed Capacity -> Only N pages can be in memory at once
    Eviction Policy -> When full, use LRU (Least Recently Used) policy
    Dirty Tracking -> Know which pages need to be written back to disk
    Pin/Unpin -> Present eviction of pages currently being used

Workflow Example:
    1. Request a page -> bufferpool.get_page(...)
    2. Cache hit -> if in mem, return (mark as most recently used)
    3. Cache miss -> load from disk (or create new)
    4. Cache full -> Evict LRU unpinned page (write to disk if dirty)
    5. Pin the page -> increment pin_count to prevent eviction while in use
    6. Use the page -> read/write data
    7. Unpin the page -> decrement pin_counter when done
"""
"""
Bufferpool manager for L-Store.
Implements LRU (Least Recently Used) page eviction policy.
"""
import os
from collections import OrderedDict
from typing import Optional, Tuple

from config import Config
from lstore.page import Page


class Bufferpool:
    """
    Manages a fixed-size pool of pages in memory with LRU eviction.
    
    Pages are identified by: (table_name, range_id, segment, page_index, column_index)
    """
    
    def __init__(self, capacity: int = 1000, base_path: str = None):
        """
        Initialize the bufferpool.
        
        :param capacity: Maximum number of pages to keep in memory
        :param base_path: Directory path for storing pages on disk
        """
        self.capacity = capacity
        self.base_path = base_path
        
        # Cache: page_key -> Page object
        # Using OrderedDict for LRU tracking
        self.cache: OrderedDict[Tuple, Page] = OrderedDict()
        
        # Statistics
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        self.writes = 0
    
    def _make_page_key(self, table_name: str, range_id: int, segment: str, 
                       page_index: int, column_index: int) -> Tuple:
        """Create a unique key for a page."""
        return (table_name, range_id, segment, page_index, column_index)
    
    def _make_file_path(self, table_name: str, range_id: int, segment: str, 
                        page_index: int, column_index: int) -> str:
        """Create the file path for a page on disk."""
        if self.base_path is None:
            return None
        filename = f"{table_name}_r{range_id}_{segment}_p{page_index}_c{column_index}.bin"
        return os.path.join(self.base_path, filename)
    
    def get_page(self, table_name: str, range_id: int, segment: str, 
                 page_index: int, column_index: int) -> Page:
        """
        Get a page from the bufferpool. Load from disk if not in cache.
        
        :return: Page object (pinned)
        """
        page_key = self._make_page_key(table_name, range_id, segment, page_index, column_index)
        
        # Check if page is in cache
        if page_key in self.cache:
            # Move to end (most recently used)
            self.cache.move_to_end(page_key)
            self.hits += 1
            page = self.cache[page_key]
            page.pin()
            return page
        
        # Page not in cache - need to load from disk
        self.misses += 1
        
        # If cache is full, evict a page
        if len(self.cache) >= self.capacity:
            self._evict_page()
        
        # Load page from disk or create new one
        page = self._load_page(table_name, range_id, segment, page_index, column_index)
        
        # Add to cache
        self.cache[page_key] = page
        page.pin()
        return page
    
    def _load_page(self, table_name: str, range_id: int, segment: str, 
                   page_index: int, column_index: int) -> Page:
        """Load a page from disk, or create a new empty page if it doesn't exist."""
        file_path = self._make_file_path(table_name, range_id, segment, page_index, column_index)
        
        # If no base path or file doesn't exist, create new page
        if file_path is None or not os.path.exists(file_path):
            return Page()
        
        # Load from disk
        try:
            with open(file_path, 'rb') as f:
                data = f.read()
                return Page.from_bytes(data)
        except Exception as e:
            # If loading fails, return empty page
            print(f"Warning: Failed to load page from {file_path}: {e}")
            return Page()
    
    def _evict_page(self):
        """Evict the least recently used unpinned page."""
        # Find first unpinned page (from least recently used)
        for page_key, page in list(self.cache.items()):
            if not page.is_pinned():
                # Flush if dirty
                if page.dirty:
                    self._write_page_to_disk(page_key, page)
                
                # Remove from cache
                del self.cache[page_key]
                self.evictions += 1
                return
        
        # If we get here, all pages are pinned - this is an error condition
        raise RuntimeError(
            f"Bufferpool deadlock: all {len(self.cache)} pages are pinned. "
            "Cannot evict any pages."
        )
    
    def _write_page_to_disk(self, page_key: Tuple, page: Page):
        """Write a page to disk."""
        if self.base_path is None:
            return
        
        table_name, range_id, segment, page_index, column_index = page_key
        file_path = self._make_file_path(table_name, range_id, segment, page_index, column_index)
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        try:
            with open(file_path, 'wb') as f:
                f.write(page.to_bytes())
            page.dirty = False
            self.writes += 1
        except Exception as e:
            print(f"Error: Failed to write page to {file_path}: {e}")
            raise
    
    def flush_page(self, table_name: str, range_id: int, segment: str, 
                   page_index: int, column_index: int):
        """Flush a specific page to disk if it's dirty."""
        page_key = self._make_page_key(table_name, range_id, segment, page_index, column_index)
        
        if page_key in self.cache:
            page = self.cache[page_key]
            if page.dirty:
                self._write_page_to_disk(page_key, page)
    
    def flush_all(self):
        """Write all dirty pages to disk (called on database close)."""
        for page_key, page in self.cache.items():
            if page.dirty:
                self._write_page_to_disk(page_key, page)
    
    def clear(self):
        """Clear the bufferpool (for testing or shutdown)."""
        self.flush_all()
        self.cache.clear()
    
    def get_stats(self):
        """Get bufferpool statistics."""
        total_accesses = self.hits + self.misses
        hit_rate = (self.hits / total_accesses * 100) if total_accesses > 0 else 0
        return {
            'capacity': self.capacity,
            'current_size': len(self.cache),
            'hits': self.hits,
            'misses': self.misses,
            'hit_rate': f"{hit_rate:.2f}%",
            'evictions': self.evictions,
            'writes': self.writes,
        }
    
    def __repr__(self):
        stats = self.get_stats()
        return (
            f"Bufferpool(size={stats['current_size']}/{stats['capacity']}, "
            f"hit_rate={stats['hit_rate']}, evictions={stats['evictions']})"
        )