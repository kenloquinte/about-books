#!/usr/bin/env bash

set -e 

# Create the DuckDB file and set permissions
python<<EOF
import duckdb
duckdb.connect(database = "db/openlibrary.duckdb", read_only = False)
EOF

chmod g=rw db/openlibrary.duckdb
