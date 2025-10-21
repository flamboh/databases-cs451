import pytest
from lstore.page import Page, MAX_SLOTS, INT_SIZE

def test_page_init():
  page = Page()
  assert page.num_records == 0

def test_page_has_capacity():
  page = Page()
  assert page.has_capacity() == True
  assert page.write(1) == 0
  for i in range(MAX_SLOTS - 1):
    assert page.has_capacity() == True
    assert page.write(i) == i + 1
  assert page.has_capacity() == False
  assert page.write(11) == False

def test_page_write():
  page = Page()
  assert page.write(1) == 0
  assert page.num_records == 1
  assert page.data[0] == 1

def test_page_write_overflow():
  page = Page()
  for i in range(MAX_SLOTS):
    page.write(i)
  assert page.num_records == MAX_SLOTS
  assert page.write(11) == False

def test_page_read():
  page = Page()
  page.write(1)
  page.write(5)
  assert page.read(0) == 1
  assert page.read(1) == 5
  assert page.read(2) == False

def test_page_read_range():
  page = Page()
  for i in range(MAX_SLOTS):
    page.write(i)
  assert page.has_capacity() == False
  assert page.read_range() == list(range(MAX_SLOTS))
  assert page.read_range(start=1, end=3) == [1, 2]
  assert page.read_range(start=0, end=MAX_SLOTS) == list(range(MAX_SLOTS))