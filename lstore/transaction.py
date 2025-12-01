import time
import random
from config import Config
import threading
from enum import Enum

class L(Enum):
    S = 0
    X = 1


class _LockManager:
    def __init__(self):
        self._lock = threading.Lock()
        self._locks = {} 
    

    def _new_lock(self, mode, owner):
        return {"mode": mode, "owners": {owner}}


    def acquire(self, txn_id, rid, mode):
        with self._lock:
            lock = self._locks.get(rid)
            if not lock:
                self._locks[rid] = self._new_lock(mode, txn_id)
                return True
            owners = lock["owners"]
            cur_mode = lock["mode"]

            if cur_mode == L.S:
                if mode == L.S:
                    owners.add(txn_id)
                    return True
                if mode == L.X and owners == {txn_id}:
                    lock["mode"] = L.X
                    return True

            elif cur_mode == L.X and owners == {txn_id}:
                return True
            return False


    def release_all(self, txn_id):
        with self._lock:
            empty = []
            for rid, entry in self._locks.items():
                owners = entry["owners"]
                owners.discard(txn_id)
                if not owners:
                    empty.append(rid)
            for rid in empty:
                del self._locks[rid]


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
        self.locked_rids = {}      # rid -> held mode

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
    def run(self, max_attempts=Config.max_attempts):
        # keep retrying until we successfully commit or attempts exhausted
        attempts = 0
        current_op = None
        while True:
            self.undo_log.clear()
            self.locked_rids.clear()
            try:
                for query_fn, table, args in self.queries:
                    op_name = query_fn.__name__
                    current_op = f"{op_name}@{getattr(table, 'name', 'unknown')}"
                    if op_name == "insert":
                        self._execute_insert(query_fn, table, args)
                    elif op_name == "update":
                        self._execute_update(query_fn, table, args)
                    elif op_name == "delete":
                        self._execute_delete(query_fn, table, args)
                    else:
                        self._execute_read(query_fn, table, args)
                return self.commit()
            except _AbortTransaction as e:
                print(f"[txn {id(self)}] abort attempt {attempts + 1} on {current_op}: {e}")
                self.abort()
                attempts += 1
                if max_attempts is not None and attempts >= max_attempts:
                    return False
                time.sleep(random.uniform(0.001, 0.01 * attempts))
                continue
            except Exception as e:
                print(
                    f"[txn {id(self)}] exception attempt {attempts + 1} on {current_op}: "
                    f"{type(e).__name__}: {e}"
                )
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
                except Exception as e:
                    print(f"[txn {id(self)}] rollback insert failed for rid {rid}: {e}")
            elif etype == "update":
                rid = entry["rid"]
                old_indirection = entry["old_indirection"]
                old_schema = entry["old_schema"]
                try:
                    self._restore_base_metadata(table, rid, old_indirection, old_schema)
                except Exception as e:
                    print(f"[txn {id(self)}] rollback update failed for rid {rid}: {e}")
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
                except Exception as e:
                    print(f"[txn {id(self)}] rollback delete failed for rid {rid}: {e}")
                    

        self._release_locks()
        return False

    
    def commit(self):
        self.undo_log.clear()
        self._release_locks()
        return True

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _acquire_lock(self, rid, mode=L.X):
        held = self.locked_rids.get(rid)
        if held:
            # already have a lock; allow if sufficient or attempt upgrade
            if held == L.X or mode == held:
                return True
            # upgrade S -> X
            ok = _GLOBAL_LOCK_MANAGER.acquire(id(self), rid, mode)
            if ok:
                self.locked_rids[rid] = mode
            return ok
        ok = _GLOBAL_LOCK_MANAGER.acquire(id(self), rid, mode)
        if ok:
            self.locked_rids[rid] = mode
        return ok

    def _release_locks(self):
        if self.locked_rids:
            _GLOBAL_LOCK_MANAGER.release_all(id(self))
            self.locked_rids.clear()

    def _execute_read(self, query_fn, table, args):
        op_name = query_fn.__name__
        rids = []
        if op_name in ("select", "select_version"):
            search_key, search_key_index = args[0], args[1]
            rids = table.index.locate(search_key_index, search_key)
        elif op_name in ("sum", "sum_version"):
            start, end = args[0], args[1]
            rids = table.index.locate_range(start, end, table.key)

        for rid in rids:
            if not self._acquire_lock(rid, L.S):
                raise _AbortTransaction(f"{op_name}: shared lock failed for rid {rid}")

        result = query_fn(*args)
        if result is False:
            raise _AbortTransaction(f"{op_name} returned False")
        return result

    def _execute_insert(self, query_fn, table, args):
        # lock allocation and write together to keep RID consistent
        with table._insert_lock:
            new_rid = table.page_directory.peek_base_rid()
            if not self._acquire_lock(new_rid, L.X):
                raise _AbortTransaction("lock acquisition failed after insert")

            result = query_fn(*args)
            if result is False:
                raise _AbortTransaction("insert returned False")

            self.undo_log.append({"type": "insert", "table": table, "rid": new_rid})

    def _execute_update(self, query_fn, table, args):
        primary_key = args[0]
        rids = table.index.locate(table.key, primary_key)
        if not rids:
            raise _AbortTransaction(f"update: no rid for key {primary_key}")
        rid = rids[0]
        if not self._acquire_lock(rid, L.X):
            raise _AbortTransaction(f"update: lock acquisition failed for rid {rid}")

        try:
            base_record = table.get_record(rid)
            cumulative = table.get_cumulative_updated_record(rid)
        except Exception:
            raise _AbortTransaction(f"update: failed to fetch record for rid {rid}")

        old_values = cumulative[Config.tail_meta_columns : Config.tail_meta_columns + table.num_columns]
        old_indirection = base_record[Config.indirection_column]
        old_schema = base_record[Config.schema_encoding_column]

        result = query_fn(*args)
        if result is False:
            raise _AbortTransaction("update returned False")

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
            raise _AbortTransaction(f"delete: no rid for key {primary_key}")
        rid = rids[0]
        if not self._acquire_lock(rid, L.X):
            raise _AbortTransaction(f"delete: lock acquisition failed for rid {rid}")

        try:
            base_record = table.get_record(rid)
            cumulative = table.get_cumulative_updated_record(rid)
        except Exception:
            raise _AbortTransaction(f"delete: failed to fetch record for rid {rid}")

        old_values = cumulative[Config.tail_meta_columns : Config.tail_meta_columns + table.num_columns]
        old_indirection = base_record[Config.indirection_column]
        old_schema = base_record[Config.schema_encoding_column]

        result = query_fn(*args)
        if result is False:
            raise _AbortTransaction("delete returned False")

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
