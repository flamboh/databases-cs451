import os
import sys
import unittest

# Ensure repository root is on sys.path when running this file directly.
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker


class TransactionSemanticsTests(unittest.TestCase):
    def setUp(self):
        self.db = Database()
        self.table = self.db.create_table("grades", 5, 0)
        self.query = Query(self.table)

    def test_insert_commits_and_persists(self):
        t = Transaction()
        t.add_query(self.query.insert, self.table, 1, 2, 3, 4, 5)

        self.assertTrue(t.run())
        row = self.query.select(1, 0, [1, 1, 1, 1, 1])[0].columns
        self.assertEqual(row, [1, 2, 3, 4, 5])

    def test_update_abort_rolls_back_prior_changes(self):
        # seed a record
        self.query.insert(1, 10, 20, 30, 40)

        t = Transaction()
        # valid update (would change col4 to 99)
        t.add_query(self.query.update, self.table, 1, None, None, None, 99)
        # invalid update: wrong column count -> returns False -> abort
        t.add_query(self.query.update, self.table, 1, None, None, None)

        self.assertFalse(t.run(max_attempts=1))
        row = self.query.select(1, 0, [1, 1, 1, 1, 1])[0].columns
        # values should remain the original ones
        self.assertEqual(row, [1, 10, 20, 30, 40])

    def test_delete_abort_restores_row_and_index(self):
        self.query.insert(1, 2, 3, 4, 5)

        t = Transaction()
        t.add_query(self.query.delete, self.table, 1)
        # force abort with invalid update (column count mismatch)
        t.add_query(self.query.update, self.table, 1, None, None, None)

        self.assertFalse(t.run(max_attempts=1))
        rows = self.query.select(1, 0, [1, 1, 1, 1, 1])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].columns, [1, 2, 3, 4, 5])

    def test_concurrent_updates_retry_and_commit(self):
        # start with a known value
        self.query.insert(1, 5, 6, 7, 8)

        t1 = Transaction()
        t1.add_query(self.query.update, self.table, 1, None, 50, None, None, None)

        t2 = Transaction()
        t2.add_query(self.query.update, self.table, 1, None, 75, None, None, None)

        w1 = TransactionWorker([t1])
        w2 = TransactionWorker([t2])

        w1.run()
        w2.run()
        w1.join()
        w2.join()

        self.assertEqual(w1.result, 1)
        self.assertEqual(w2.result, 1)

        # final value should reflect one of the successful updates
        row = self.query.select(1, 0, [1, 1, 1, 1, 1])[0].columns
        self.assertIn(row[1], (50, 75))


if __name__ == "__main__":
    unittest.main()
