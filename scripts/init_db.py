# filepath: scripts/init_db.py

"""Entry point for database initialization."""

import sys
import os

# Add project root to path
project_root = os.path.join(os.path.dirname(__file__), '..')
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from local.src.database.init_db import main

if __name__ == "__main__":
    main()