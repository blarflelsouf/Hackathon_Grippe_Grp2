#!/bin/bash
set -e
# Init Superset
superset fab create-admin --username admin --firstname Admin --lastname User --email admin@example.com --password admin || true
superset db upgrade
superset init
# Add a SQLite dataset from processed files via CSV upload is manual.
# Optionally, point Superset to a Postgres where you load processed CSV.
gunicorn -w 2 -k gevent --timeout 120 --limit-request-line 0 --limit-request-field_size 0 "superset.app:create_app()" -b 0.0.0.0:8088
