import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from lstore.bplus import BPlusTree


def test_insert_and_duplicate_values():
    tree = BPlusTree(order=4)

    print("Running insert/duplicate test...")
    tree.insert(10, 1)
    tree.insert(10, 2)
    print("Inserted values 1 and 2 for key 10")
    tree.insert(10, 1)  # duplicate value should be ignored
    print("Attempted to insert duplicate value 1 for key 10")
    tree.insert(5, 99)
    print("Inserted key 5 with value 99")

    assert tree.find(10) == [1, 2]
    assert tree.find(5) == [99]
    assert tree.find(0) == []
    assert len(tree) == 3
    assert list(tree.items()) == [(5, [99]), (10, [1, 2])]


def test_range_query_across_leaf_chain():
    tree = BPlusTree(order=4)

    print("Running range query test...")
    for key in range(1, 13):
        tree.insert(key, key * 10)
    print("Inserted keys 1 through 12 with values key*10")

    assert tree.find_range(3, 8) == [30, 40, 50, 60, 70, 80]
    assert tree.find_range(0, 2) == [10, 20]
    assert tree.find_range(9, 8) == []


def test_remove_value_and_rebalance():
    tree = BPlusTree(order=3)

    print("Running deletion/rebalance test...")
    for key in range(1, 8):
        tree.insert(key, key)
    print("Inserted keys 1 through 7 with matching values")
    tree.insert(4, 999)
    print("Inserted additional value 999 for key 4")

    assert len(tree) == 8
    assert tree.remove(4, 999)
    print("Removed value 999 from key 4")
    assert tree.find(4) == [4]
    assert len(tree) == 7

    assert tree.remove(4)
    print("Removed remaining values for key 4")
    assert tree.find(4) == []
    assert len(tree) == 6

    for key in (2, 3, 5):
        print(f"Removing key {key}")
        assert tree.remove(key)

    assert list(tree.items()) == [(1, [1]), (6, [6]), (7, [7])]
    assert tree.find_range(1, 7) == [1, 6, 7]
    assert tree.remove(2) is False


if __name__ == "__main__":
    test_insert_and_duplicate_values()
    test_range_query_across_leaf_chain()
    test_remove_value_and_rebalance()
