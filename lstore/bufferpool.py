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