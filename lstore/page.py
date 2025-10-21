PAGE_SIZE = 4096
INT_SIZE = 8
MAX_SLOTS = PAGE_SIZE // INT_SIZE

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
        self.data = bytearray(PAGE_SIZE)
        self.capacity = MAX_SLOTS

    def has_capacity(self):
        """
        Checks if there is remaining capacity in the page for a new record.

        :return: True if there is room for at least one more record, False otherwise.
        """
        return self.num_records < MAX_SLOTS

    def get_offset(self, index):
        """
        Gets the offset of the specified slot in the page.
        
        :param index: Slot index to get the offset of.
        :return: The offset of the specified slot.
        """
        return index * INT_SIZE
    

    def write(self, value):
        """
        Writes a 64-bit signed integer to the next free slot in the page.
        
        :param value: Integer value to write.
        :return: The index (slot) where the value was written, or False if the page is full.
        """
        if not self.has_capacity():
            return False
        offset = self.get_offset(self.num_records)
        self.data[offset:offset + INT_SIZE] = value.to_bytes(INT_SIZE, byteorder='little', signed=True)
        self.num_records += 1
        return self.num_records - 1
    
    def read(self, index):
        """
        Reads a 64-bit signed integer from the specified slot.

        :param index: Slot index to read from.
        :return: The integer value at the specified slot.
        """
        if index < 0 or index >= self.num_records:
            raise IndexError(f"Index {index} out of bounds [0, {self.num_records})")
        offset = self.get_offset(index)
        return int.from_bytes(self.data[offset:offset + INT_SIZE], byteorder='little', signed=True)

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