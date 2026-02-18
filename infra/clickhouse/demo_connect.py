# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "argparse",
#   "clickhouse-connect",
# ]
# ///

import argparse
import time

import clickhouse_connect

parser = argparse.ArgumentParser()
parser.add_argument('--host', type=str, required=True)
parser.add_argument('--port', type=int, required=True)
parser.add_argument('--username', type=str, required=True)
parser.add_argument('--password', type=str, required=True)
args = parser.parse_args()

client = clickhouse_connect.get_client(
    host=args.host, 
    port=args.port, 
    secure = 'https',
    verify = 'True',
    username=args.username, 
    password=args.password,
    database='analytics',
)

start = time.perf_counter()
result = client.query('SELECT count(*) FROM analytics.zap_events')
elapsed = time.perf_counter() - start
print(f"Query took {elapsed:.3f}s, result rows: {result.result_rows}")