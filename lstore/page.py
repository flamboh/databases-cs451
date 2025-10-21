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
        self.capacity = Config.max_slots

    def has_capacity(self):
        """
        Checks if there is remaining capacity in the page for a new record.

        :return: True if there is room for at least one more record, False otherwise.
        """
        return self.num_records < Config.max_slots

    def get_offset(self, slot):
        """
        Gets the offset of the specified slot in the page.
        
        :param slot: Slot index to get the offset of.
        :return: The offset of the specified slot.
        """
        return slot * Config.int_size
    

    def write(self, value):
        """
        Writes a 64-bit signed integer to the next free slot in the page.
        
        :param value: Integer value to write.
        :return: The index (slot) where the value was written, or False if the page is full.
        """
        if not self.has_capacity():
            return False
        slot_offset = self.get_offset(self.num_records)
        self.data[slot_offset:slot_offset + Config.int_size] = value.to_bytes(Config.int_size, byteorder=Config.byteorder, signed=True)
        self.num_records += 1
        return self.num_records - 1
    
    def read(self, slot):
        """
        Reads a 64-bit signed integer from the specified slot.

        :param slot: Slot index to read from.
        :return: The integer value at the specified slot.
        """
        if slot < 0 or slot >= self.num_records:
            raise IndexError(f"Index {slot} out of bounds [0, {self.num_records})")
        slot_offset = self.get_offset(slot)
        return int.from_bytes(self.data[slot_offset:slot_offset + Config.int_size], byteorder=Config.byteorder, signed=True)

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
        return [self.read(i) for i in range(start, end)]