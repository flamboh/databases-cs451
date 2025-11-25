from lstore.table import Table, Record
from lstore.index import Index
from config import Config
import threading


class _LockManager:
    """
    Extremely small, exclusive-only lock manager with no-wait semantics.
    Maps RID -> owning transaction id. If a lock is held by another txn,
    acquisition fails immediately and the caller should abort.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._owners = {}

    def acquire(self, txn_id, rid):
        with self._lock:
            owner = self._owners.get(rid)
            if owner is None or owner == txn_id:
                self._owners[rid] = txn_id
                return True
            return False

    def release_all(self, txn_id):
        with self._lock:
            rids = [rid for rid, owner in self._owners.items() if owner == txn_id]
            for rid in rids:
                del self._owners[rid]


_GLOBAL_LOCK_MANAGER = _LockManager()


class _AbortTransaction(Exception):
    """Internal control-flow exception to trigger an abort and retry."""
    pass

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []          # (query_fn, table, args)
        self.undo_log = []         # list of undo entries
        self.locked_rids = set()   # RIDs locked by this transaction

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, table, args))
        # store table for lock/undo context

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self, max_attempts=None):
        # keep retrying until we successfully commit or attempts exhausted
        attempts = 0
        while True:
            self.undo_log.clear()
            self.locked_rids.clear()
            try:
                for query_fn, table, args in self.queries:
                    op_name = query_fn.__name__
                    if op_name == "insert":
                        self._execute_insert(query_fn, table, args)
                    elif op_name == "update":
                        self._execute_update(query_fn, table, args)
                    elif op_name == "delete":
                        self._execute_delete(query_fn, table, args)
                    else:
                        # treat other operations (e.g., select, sum) as read-only
                        result = query_fn(*args)
                        if result is False:
                            raise _AbortTransaction()
                return self.commit()
            except _AbortTransaction:
                self.abort()
                attempts += 1
                if max_attempts is not None and attempts >= max_attempts:
                    return False
                continue
            except Exception:
                self.abort()
                attempts += 1
                if max_attempts is not None and attempts >= max_attempts:
                    return False
                continue

    
    def abort(self):
        # rollback in reverse order
        for entry in reversed(self.undo_log):
            etype = entry["type"]
            table = entry["table"]
            if etype == "insert":
                # remove the inserted record
                rid = entry["rid"]
                try:
                    table.delete_record(rid)
                except Exception:
                    continue
            elif etype == "update":
                rid = entry["rid"]
                old_values = entry["old_values"]
                try:
                    tail_meta = [Config.null_value for _ in range(Config.tail_meta_columns)]
                    tail_record = tail_meta + list(old_values)
                    table.insert_record(tail_record, is_tail=True, base_rid=rid)
                except Exception:
                    continue
            elif etype == "delete":
                rid = entry["rid"]
                old_values = entry["old_values"]
                old_indirection = entry["old_indirection"]
                old_schema = entry["old_schema"]
                try:
                    # restore index entry
                    table.index.add(rid, old_values)
                    # restore base metadata
                    self._restore_base_metadata(table, rid, old_indirection, old_schema)
                except Exception:
                    continue

        self._release_locks()
        return False

    
    def commit(self):
        self.undo_log.clear()
        self._release_locks()
        return True

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _acquire_lock(self, rid):
        if rid in self.locked_rids:
            return True
        ok = _GLOBAL_LOCK_MANAGER.acquire(id(self), rid)
        if ok:
            self.locked_rids.add(rid)
        return ok

    def _release_locks(self):
        if self.locked_rids:
            _GLOBAL_LOCK_MANAGER.release_all(id(self))
            self.locked_rids.clear()

    def _execute_insert(self, query_fn, table, args):
        result = query_fn(*args)
        if result is False:
            raise _AbortTransaction()

        # find the newly inserted base RID using the primary key
        primary_key = args[table.key]
        rids = table.index.locate(table.key, primary_key)
        if not rids:
            raise _AbortTransaction()
        rid = rids[0]
        if not self._acquire_lock(rid):
            raise _AbortTransaction()

        self.undo_log.append({"type": "insert", "table": table, "rid": rid})

    def _execute_update(self, query_fn, table, args):
        primary_key = args[0]
        rids = table.index.locate(table.key, primary_key)
        if not rids:
            raise _AbortTransaction()
        rid = rids[0]
        if not self._acquire_lock(rid):
            raise _AbortTransaction()

        try:
            base_record = table.get_record(rid)
            cumulative = table.get_cumulative_updated_record(rid)
        except Exception:
            raise _AbortTransaction()

        old_values = cumulative[Config.tail_meta_columns : Config.tail_meta_columns + table.num_columns]
        old_indirection = base_record[Config.indirection_column]
        old_schema = base_record[Config.schema_encoding_column]

        result = query_fn(*args)
        if result is False:
            raise _AbortTransaction()

        self.undo_log.append(
            {
                "type": "update",
                "table": table,
                "rid": rid,
                "old_values": old_values,
                "old_indirection": old_indirection,
                "old_schema": old_schema,
            }
        )

    def _execute_delete(self, query_fn, table, args):
        primary_key = args[0]
        rids = table.index.locate(table.key, primary_key)
        if not rids:
            raise _AbortTransaction()
        rid = rids[0]
        if not self._acquire_lock(rid):
            raise _AbortTransaction()

        try:
            base_record = table.get_record(rid)
            cumulative = table.get_cumulative_updated_record(rid)
        except Exception:
            raise _AbortTransaction()

        old_values = cumulative[Config.tail_meta_columns : Config.tail_meta_columns + table.num_columns]
        old_indirection = base_record[Config.indirection_column]
        old_schema = base_record[Config.schema_encoding_column]

        result = query_fn(*args)
        if result is False:
            raise _AbortTransaction()

        self.undo_log.append(
            {
                "type": "delete",
                "table": table,
                "rid": rid,
                "old_values": old_values,
                "old_indirection": old_indirection,
                "old_schema": old_schema,
            }
        )

    def _restore_base_metadata(self, table, rid, indirection, schema):
        """Write the base record's indirection and schema back to prior values."""
        range_id, segment, page_index, slot_index = table.page_directory.decode_rid(rid)
        if segment != 0:
            return False
        with table.page_directory._column(range_id, 0, page_index, Config.indirection_column) as ind_page:
            ind_page.write_slot(slot_index, indirection)
        with table.page_directory._column(range_id, 0, page_index, Config.schema_encoding_column) as schema_page:
            schema_page.write_slot(slot_index, schema)
        return True
