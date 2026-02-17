#!/usr/bin/env sh
# Source DLT env (maps raw vars to DESTINATION__* / SOURCES__* etc.) then run CMD.
set -e
. /app/infra/dlt/set_dlt_env.sh
exec "$@"
