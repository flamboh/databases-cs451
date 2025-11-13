import json
import shutil
from pathlib import Path
from typing import Optional

from lstore.bufferpool import Bufferpool
from lstore.table import Table


class Database:
    def __init__(self, bufferpool_capacity: int = 1000):
        self.tables: dict[str, Table] = {}
        self._bufferpool_capacity = bufferpool_capacity
        self.bufferpool = Bufferpool(capacity=bufferpool_capacity)
        self.path: Optional[Path] = None
        self.pages_path: Optional[Path] = None
        self.metadata_path: Optional[Path] = None

    def open(self, path: str):
        """Open database and initialize bufferpool from disk."""
        if self.tables:
            raise RuntimeError("Cannot open a persistent database after tables have been created")

        base_path = Path(path)
        base_path.mkdir(parents=True, exist_ok=True)
        pages_path = base_path / "pages"
        pages_path.mkdir(parents=True, exist_ok=True)

        self.path = base_path
        self.pages_path = pages_path
        self.metadata_path = base_path / "metadata.json"
        self.bufferpool = Bufferpool(capacity=self._bufferpool_capacity, base_path=pages_path)
        self._load_metadata()

    def close(self):
        """Flush all pages and save metadata."""
        for table in self.tables.values():
            table.close()
        if self.bufferpool:
            self.bufferpool.flush_all()
        self._save_metadata()

    # ------------------------------------------------------------------
    # Table lifecycle
    # ------------------------------------------------------------------
    def create_table(self, name, num_columns, key_index):
        if name in self.tables:
            raise ValueError(f"Table {name} already exists")
        table = Table(name, num_columns, key_index, bufferpool=self.bufferpool)
        self.tables[name] = table
        self._save_metadata()
        return table

    def drop_table(self, name):
        if name not in self.tables:
            raise ValueError(f"Table {name} does not exist")

        # Remove table metadata and cached pages before deleting files.
        table = self.tables.pop(name)
        table.close()
        if self.bufferpool:
            self.bufferpool.discard_table(name)

        if self.pages_path:
            table_dir = self.pages_path / name
            if table_dir.exists():
                shutil.rmtree(table_dir)

        self._save_metadata()
        return True

    def get_table(self, name):
        if name not in self.tables:
            raise ValueError(f"Table {name} does not exist")
        return self.tables[name]

    # ------------------------------------------------------------------
    # Metadata helpers
    # ------------------------------------------------------------------
    def _load_metadata(self):
        if not self.metadata_path or not self.metadata_path.exists():
            return
        with open(self.metadata_path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        for table_name, metadata in data.get("tables", {}).items():
            directory_metadata = metadata.get("directory")
            table = Table(
                table_name,
                metadata["num_columns"],
                metadata["key"],
                bufferpool=self.bufferpool,
                directory_metadata=directory_metadata,
            )
            self.tables[table_name] = table

    def _save_metadata(self):
        if not self.metadata_path:
            return
        data = {"tables": {name: table.to_metadata() for name, table in self.tables.items()}}
        self.metadata_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.metadata_path, "w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=2)
