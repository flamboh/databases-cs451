import pytest

from config import Config
from lstore.page import Page


def test_page_init():
    page = Page()
    assert page.num_records == 0


def test_page_has_capacity():
    page = Page()
    assert page.has_capacity()
    assert page.write(1) == 0

    for value in range(Config.records_per_page - 1):
        assert page.has_capacity()
        assert page.write(value) == value + 1

    assert not page.has_capacity()
    assert page.write(11) is False


def test_page_write():
    page = Page()
    assert page.write(1) == 0
    assert page.num_records == 1
    assert page.read(0) == 1


def test_page_write_overflow():
    page = Page()

    for value in range(Config.records_per_page):
        page.write(value)

    assert page.num_records == Config.records_per_page
    assert page.write(11) is False


def test_page_read():
    page = Page()
    page.write(1)
    page.write(5)

    assert page.read(0) == 1
    assert page.read(1) == 5

    with pytest.raises(IndexError):
        page.read(2)


def test_page_read_range():
    page = Page()

    for value in range(5):
        page.write(value * 10)

    assert page.read_range(2, 4) == [20, 30]
    assert page.read_range(0) == [0, 10, 20, 30, 40]


def test_page_read_range_max_slots():
    page = Page()

    for value in range(Config.records_per_page):
        page.write(value)

    assert not page.has_capacity()
    assert page.read_range() == list(range(Config.records_per_page))
    assert page.read_range(start=1, end=3) == [1, 2]
    assert page.read_range(0) == list(range(Config.records_per_page))


def test_page_write_read_negative():
    page = Page()
    page.write(-42)
    page.write(-999_999)

    assert page.read(0) == -42
    assert page.read(1) == -999_999


def test_page_write_read_boundary_values():
    page = Page()
    max_int64 = 2**63 - 1
    min_int64 = -(2**63)

    page.write(max_int64)
    page.write(min_int64)

    assert page.read(0) == max_int64
    assert page.read(1) == min_int64


def test_page_read_range_edge_cases():
    page = Page()

    for value in range(5):
        page.write(value * 10)

    assert page.read_range(2, 2) == []
    assert page.read_range(0, 0) == []
    assert page.read_range(5, 5) == []

    with pytest.raises(IndexError):
        page.read_range(2, 1)
    with pytest.raises(IndexError):
        page.read_range(-1, 2)
    with pytest.raises(IndexError):
        page.read_range(2, 10)
    with pytest.raises(IndexError):
        page.read_range(10, 2)
