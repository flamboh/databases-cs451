from config import Config


class Page:
    """
    The Page class represents a fixed-size columnar page, storing 64-bit integer values.
    Supports basic record management including write, read, and range-read operations.
    """

    def __init__(self):
        """
        Initializes an empty page with capacity for MAX_SLOTS 64-bit integers.
        """
        self.num_records = 0
        self.data = bytearray(Config.page_size)
        self.page_id = id(self)
        self.capacity = Config.records_per_page
        
        # Bufferpool support
        self.dirty = False # track to see if page has been modified
        self.pin = 0 # track how many transactions are using this page

    def has_capacity(self):
        """
        Checks if there is remaining capacity in the page for a new record.

        :return: True if there is room for at least one more record, False otherwise.
        """
        return self.num_records < Config.records_per_page

    def get_offset(self, slot_index):
        """
        Gets the offset of the specified slot in the page.
        
        :param slot_index: Slot index to get the offset of.
        :return: The offset of the specified slot.
        """
        return slot_index * Config.int_size
    
    def pin(self):
        """Pin this page to prevent eviction while in use."""
        self.pin_count += 1
    
    def unpin(self):
        """Unpin this page when done using it."""
        self.pin_count = max(0, self.pin_count - 1)
    
    def is_pinned(self):
        """Check if this page is currently pinned."""
        return self.pin_count > 0

    def write(self, value):
        """
        Writes a 64-bit signed integer to the next free slot in the page.
        
        :param value: Integer value to write.
        :return: The index (slot) where the value was written, or False if the page is full.
        """
        if not self.has_capacity():
            return False
        slot_offset = self.get_offset(self.num_records)
        self.data[slot_offset:slot_offset + Config.int_size] = value.to_bytes(
            Config.int_size,
            byteorder=Config.byteorder,
            signed=True,
        )
        self.num_records += 1
        self.dirty = True # mark as dirty after write
        return self.num_records - 1

    def write_slot(self, slot_index, value):
        """
        Writes a 64-bit signed integer to the specified slot in the page.
        
        :param slot_index: Slot index to write to.
        :param value: Integer value to write.
        :return: The index (slot) where the value was written, or False if the page is full.
        """
        if slot_index < 0 or slot_index >= Config.records_per_page:
            raise IndexError(f"Index {slot_index} out of bounds [0, {Config.records_per_page})")

        # Writes must either extend the column by one (append) or target an existing slot.
        if slot_index > self.num_records:
            raise IndexError(
                f"Cannot write to slot {slot_index}: current length is {self.num_records}."
            )

        if slot_index == self.num_records:
            if not self.has_capacity():
                return False
            slot_offset = self.get_offset(slot_index)
            self.data[slot_offset:slot_offset + Config.int_size] = value.to_bytes(
                Config.int_size,
                byteorder=Config.byteorder,
                signed=True,
            )
            self.num_records += 1
            self.dirty = True # mark as dirty after write
            return slot_index

        slot_offset = self.get_offset(slot_index)
        self.data[slot_offset:slot_offset + Config.int_size] = value.to_bytes(
            Config.int_size,
            byteorder=Config.byteorder,
            signed=True,
        )
        self.dirty = True # mark as dirty after write
        return slot_index

    def read(self, slot_index):
        """
        Reads a 64-bit signed integer from the specified slot.

        :param slot_index: Slot index to read from.
        :return: The integer value at the specified slot.
        """
        if slot_index < 0 or slot_index >= self.num_records:
            raise IndexError(f"Index {slot_index} out of bounds [0, {self.num_records})")
        slot_offset = self.get_offset(slot_index)
        return int.from_bytes(
            self.data[slot_offset:slot_offset + Config.int_size],
            byteorder=Config.byteorder,
            signed=True,
        )

    def read_range(self, start=0, end=None):
        """
        Reads a sequence of 64-bit integers from a range of slots.

        :param start: The starting index (inclusive).
        :param end: The ending index (exclusive). Defaults to num_records if None
        :return: A list of integer values from start to end-1 slots.
        """
        if end is None:
            end = self.num_records
        if start < 0 or start > self.num_records or end < 0 or end > self.num_records or start > end:
            raise IndexError(f"Invalid range [{start}, {end}) out of bounds [0, {self.num_records}) or start > end")
        return [self.read(slot_index) for slot_index in range(start, end)]

    # ------------------------------------------------------------------
    # Serialization for disk persistence
    # ------------------------------------------------------------------
    
    def to_bytes(self):
        """
        Serialize this page to bytes for writing to disk.
        Returns the raw page data plus metadata.
        
        :return: bytes object containing page data and metadata
        """
        # Format: [num_records (8 bytes)][page data (4096 bytes)]
        header = self.num_records.to_bytes(8, byteorder=Config.byteorder, signed=False)
        return header + bytes(self.data)
    
    @staticmethod
    def from_bytes(data):
        """
        Deserialize a page from bytes read from disk.
        
        :param data: bytes object containing serialized page
        :return: Page object
        """
        page = Page()
        # Read header
        page.num_records = int.from_bytes(data[0:8], byteorder=Config.byteorder, signed=False)
        # Read page data
        page.data = bytearray(data[8:8+Config.page_size])
        page.dirty = False  # Fresh from disk, so not dirty
        return page
    
    def __repr__(self):
        """Provide a concise, human-readable view when pages are printed."""
        return (
            f"Page(id={self.page_id}, records={self.num_records}, "
            f"capacity={self.capacity})"
        )

    __str__ = __repr__
