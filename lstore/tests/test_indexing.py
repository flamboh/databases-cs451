from lstore.db import Database
from lstore.query import Query

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

def test_indexing():
  db = Database()
  table = db.create_table('Test', 3, 0)
  query = Query(table)

  # Insert some records
  query.insert(1, 100, 200)
  query.insert(2, 100, 300)  # Note: same value in column 1
  query.insert(3, 150, 400)

  # Create secondary index on column 1
  assert table.index.create_index(1)

  # Should find both records with value 100 in column 1
  results = query.select(100, 1, [1, 1, 1])
  assert len(results) == 2
  print(f"Found {len(results)} records")  # Should print: Found 2 records

  # Try to create the same index again - should return False
  assert not table.index.create_index(1)

  # Drop the index
  assert table.index.drop_index(1)

  # Can't drop primary key index
  assert not table.index.drop_index(0)
