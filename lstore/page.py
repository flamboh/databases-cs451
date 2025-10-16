PAGE_SIZE = 4096
INT_SIZE = 8
MAX_SLOTS = PAGE_SIZE // INT_SIZE

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)
        self.capacity = MAX_SLOTS

    def has_capacity(self):
        return self.num_records < MAX_SLOTS

    def write(self, value):
        if not self.has_capacity():
            return False
        offset = self.num_records * INT_SIZE
        self.data[offset:offset + INT_SIZE] = value.to_bytes(INT_SIZE, byteorder='little', signed=True)
        self.num_records += 1
        return self.num_records - 1

