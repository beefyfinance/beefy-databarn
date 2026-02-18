# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "argparse",
#   "clickhouse-connect",
# ]
# ///

import clickhouse_connect
import argparse

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

result = client.query('SELECT count(*) FROM analytics.zap_events')
print(result.result_rows)