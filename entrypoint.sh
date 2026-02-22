#!/usr/bin/env sh
set -e

# Forward all Cloud Run "args" to the python script
exec /usr/local/bin/python /app/backfill.py "$@"
