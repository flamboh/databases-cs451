"""Lightweight in-memory B+ tree implementation for integer keys.

The tree is fully generic with respect to the values stored for each key; the
Index class uses it to map column values to record identifiers (RIDs).
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from typing import Iterable, List, Optional, Sequence


class _Node:
    """Base class shared by internal and leaf nodes."""

    def __init__(self, order: int) -> None:
        self.order = order
        self.keys: List[int] = []
        self.parent: Optional[_InternalNode] = None

    def is_leaf(self) -> bool:
        return False


class _LeafNode(_Node):
    """Leaf node storing keys and their associated value buckets."""

    def __init__(self, order: int) -> None:
        super().__init__(order)
        self.values: List[List[int]] = []
        self.next: Optional[_LeafNode] = None
        self.prev: Optional[_LeafNode] = None

    def is_leaf(self) -> bool:
        return True


class _InternalNode(_Node):
    """Internal node storing separator keys and child pointers."""

    def __init__(self, order: int) -> None:
        super().__init__(order)
        self.children: List[_Node] = []


class BPlusTree:
    """B+ tree supporting insert, search, range queries, and deletion."""

    def __init__(self, order: int = 32) -> None:
        if order < 3:
            raise ValueError("order must be >= 3")
        self.order = order
        self._root: _Node = _LeafNode(order)
        self._size = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def __len__(self) -> int:
        return self._size

    def insert(self, key: int, value: int) -> None:
        leaf = self._find_leaf(key)
        idx = bisect_left(leaf.keys, key)

        if idx < len(leaf.keys) and leaf.keys[idx] == key:
            bucket = leaf.values[idx]
            if value not in bucket:
                bucket.append(value)
                self._size += 1
            return

        leaf.keys.insert(idx, key)
        leaf.values.insert(idx, [value])
        self._size += 1

        if len(leaf.keys) > self._max_keys:
            self._split_leaf(leaf)

    def find(self, key: int) -> List[int]:
        leaf = self._find_leaf(key)
        idx = bisect_left(leaf.keys, key)
        if idx < len(leaf.keys) and leaf.keys[idx] == key:
            return list(leaf.values[idx])
        return []

    def find_range(self, start: int, end: int) -> List[int]:
        if start > end:
            return []
        leaf = self._find_leaf(start)
        results: List[int] = []
        current: Optional[_LeafNode] = leaf
        while current is not None:
            for key, bucket in zip(current.keys, current.values):
                if key > end:
                    return results
                if key >= start:
                    results.extend(bucket)
            current = current.next
        return results

    def remove(self, key: int, value: Optional[int] = None) -> bool:
        leaf = self._find_leaf(key)
        idx = bisect_left(leaf.keys, key)
        if idx >= len(leaf.keys) or leaf.keys[idx] != key:
            return False

        bucket = leaf.values[idx]
        removed = False
        if value is None:
            removed = bool(bucket)
            self._size -= len(bucket)
            bucket.clear()
        else:
            try:
                bucket.remove(value)
                self._size -= 1
                removed = True
            except ValueError:
                return False

        if bucket:
            return removed

        # Entire key removed from leaf; update tree structure.
        leaf.keys.pop(idx)
        leaf.values.pop(idx)
        self._rebalance_after_delete(leaf)
        return removed

    def items(self) -> Iterable[Sequence[int]]:
        node = self._leftmost_leaf()
        while node is not None:
            for key, values in zip(node.keys, node.values):
                yield key, list(values)
            node = node.next

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @property
    def _max_keys(self) -> int:
        return self.order - 1

    @property
    def _min_leaf_keys(self) -> int:
        return (self._max_keys + 1) // 2

    @property
    def _min_internal_keys(self) -> int:
        return ((self.order + 1) // 2) - 1

    def _leftmost_leaf(self) -> _LeafNode:
        node = self._root
        while not node.is_leaf():
            node = node.children[0]
        return node  # type: ignore[return-value]

    def _find_leaf(self, key: int) -> _LeafNode:
        node = self._root
        while not node.is_leaf():
            internal = node  # type: ignore[assignment]
            child_index = bisect_right(internal.keys, key)
            node = internal.children[child_index]
        return node  # type: ignore[return-value]

    def _split_leaf(self, leaf: _LeafNode) -> None:
        mid = len(leaf.keys) // 2
        sibling = _LeafNode(self.order)
        sibling.keys = leaf.keys[mid:]
        sibling.values = leaf.values[mid:]
        leaf.keys = leaf.keys[:mid]
        leaf.values = leaf.values[:mid]

        sibling.next = leaf.next
        if sibling.next is not None:
            sibling.next.prev = sibling
        leaf.next = sibling
        sibling.prev = leaf
        sibling.parent = leaf.parent

        split_key = sibling.keys[0]
        self._insert_into_parent(leaf, split_key, sibling)

    def _split_internal(self, node: _InternalNode) -> None:
        mid_index = len(node.keys) // 2
        promote_key = node.keys[mid_index]

        sibling = _InternalNode(self.order)
        sibling.keys = node.keys[mid_index + 1 :]
        sibling.children = node.children[mid_index + 1 :]
        for child in sibling.children:
            child.parent = sibling

        node.keys = node.keys[:mid_index]
        node.children = node.children[: mid_index + 1]
        for child in node.children:
            child.parent = node

        sibling.parent = node.parent
        self._insert_into_parent(node, promote_key, sibling)

    def _insert_into_parent(self, left: _Node, key: int, right: _Node) -> None:
        right.parent = left.parent
        if left.parent is None:
            new_root = _InternalNode(self.order)
            new_root.keys = [key]
            new_root.children = [left, right]
            left.parent = new_root
            right.parent = new_root
            self._root = new_root
            return

        parent = left.parent
        insert_pos = parent.children.index(left) + 1
        parent.children.insert(insert_pos, right)
        parent.keys.insert(insert_pos - 1, key)

        if len(parent.keys) > self._max_keys:
            self._split_internal(parent)
        else:
            self._sync_parent_keys(parent)

    def _rebalance_after_delete(self, node: _Node) -> None:
        if node.parent is None:
            if not node.is_leaf() and len(node.children) == 1:
                self._root = node.children[0]
                self._root.parent = None
            elif node.is_leaf() and not node.keys:
                # Tree becomes empty; root remains a leaf.
                self._root = node
            return

        min_keys = self._min_leaf_keys if node.is_leaf() else self._min_internal_keys
        if len(node.keys) >= min_keys:
            self._sync_parent_keys(node.parent)
            return

        parent = node.parent
        index = parent.children.index(node)
        left = parent.children[index - 1] if index > 0 else None
        right = parent.children[index + 1] if index + 1 < len(parent.children) else None

        if left is not None and len(left.keys) > (self._min_leaf_keys if left.is_leaf() else self._min_internal_keys):
            self._borrow_from_left(node, left, parent, index)
        elif right is not None and len(right.keys) > (self._min_leaf_keys if right.is_leaf() else self._min_internal_keys):
            self._borrow_from_right(node, right, parent, index)
        else:
            if left is not None:
                self._merge_nodes(left, node, parent, index - 1)
            elif right is not None:
                self._merge_nodes(node, right, parent, index)

    def _borrow_from_left(self, node: _Node, left: _Node, parent: _InternalNode, index: int) -> None:
        if node.is_leaf():
            leaf = node  # type: ignore[assignment]
            left_leaf = left  # type: ignore[assignment]
            leaf.keys.insert(0, left_leaf.keys.pop())
            leaf.values.insert(0, left_leaf.values.pop())
            parent.keys[index - 1] = leaf.keys[0]
        else:
            internal = node  # type: ignore[assignment]
            left_internal = left  # type: ignore[assignment]
            borrow_key = parent.keys[index - 1]
            borrow_child = left_internal.children.pop()
            internal.keys.insert(0, borrow_key)
            internal.children.insert(0, borrow_child)
            borrow_child.parent = internal
            parent.keys[index - 1] = left_internal.keys.pop()
        self._sync_parent_keys(parent)

    def _borrow_from_right(self, node: _Node, right: _Node, parent: _InternalNode, index: int) -> None:
        if node.is_leaf():
            leaf = node  # type: ignore[assignment]
            right_leaf = right  # type: ignore[assignment]
            leaf.keys.append(right_leaf.keys.pop(0))
            leaf.values.append(right_leaf.values.pop(0))
            parent.keys[index] = right_leaf.keys[0] if right_leaf.keys else parent.keys[index]
        else:
            internal = node  # type: ignore[assignment]
            right_internal = right  # type: ignore[assignment]
            borrow_key = parent.keys[index]
            borrow_child = right_internal.children.pop(0)
            internal.keys.append(borrow_key)
            internal.children.append(borrow_child)
            borrow_child.parent = internal
            parent.keys[index] = right_internal.keys.pop(0)
        self._sync_parent_keys(parent)

    def _merge_nodes(self, left: _Node, right: _Node, parent: _InternalNode, parent_index: int) -> None:
        if left.is_leaf():
            left_leaf = left  # type: ignore[assignment]
            right_leaf = right  # type: ignore[assignment]
            left_leaf.keys.extend(right_leaf.keys)
            left_leaf.values.extend(right_leaf.values)
            left_leaf.next = right_leaf.next
            if right_leaf.next is not None:
                right_leaf.next.prev = left_leaf
        else:
            left_internal = left  # type: ignore[assignment]
            right_internal = right  # type: ignore[assignment]
            separator = parent.keys[parent_index]
            left_internal.keys.append(separator)
            left_internal.keys.extend(right_internal.keys)
            left_internal.children.extend(right_internal.children)
            for child in right_internal.children:
                child.parent = left_internal

        parent.keys.pop(parent_index)
        parent.children.remove(right)

        if parent.parent is None and not parent.keys:
            self._root = left
            left.parent = None
            return

        self._rebalance_after_delete(parent)

    def _sync_parent_keys(self, parent: Optional[_InternalNode]) -> None:
        while parent is not None:
            for i in range(len(parent.keys)):
                parent.keys[i] = self._get_first_key(parent.children[i + 1])
            parent = parent.parent

    def _get_first_key(self, node: _Node) -> int:
        current = node
        while not current.is_leaf():
            current = current.children[0]  # type: ignore[assignment]
        return current.keys[0]  # type: ignore[return-value]


# Backwards compatibility alias for existing imports in the project.
bPlusTree = BPlusTree
