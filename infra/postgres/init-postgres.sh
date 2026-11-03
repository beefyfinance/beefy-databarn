#!/bin/bash
set -e

# Initialize Postgres databases for Grafana
# This script is automatically executed on first startup via /docker-entrypoint-initdb.d/

# Check if a database exists
database_exists() {
    local db_name=$1
    psql -tAc --username "$POSTGRES_USER" -c "SELECT 1 FROM pg_database WHERE datname='$db_name'" 2>/dev/null | grep -q 1
}

# Create a database if it doesn't exist (idempotent)
create_database_if_not_exists() {
    local db_name=$1
    local description=$2
    
    if database_exists "$db_name"; then
        echo "$description database $db_name already exists, skipping creation"
    else
        echo "Creating $description database: $db_name"
        psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
            CREATE DATABASE $db_name;
EOSQL
    fi
}

# Create/update a user and set as owner of a database
create_user_and_grant_permissions() {
    local db_name=$1
    local user_name=$2
    local password=$3
    local description=$4
    
    echo "Creating/updating $description user: $user_name"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
        DO \$\$
        BEGIN
            IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = '$user_name') THEN
                CREATE USER $user_name WITH PASSWORD '$password';
            ELSE
                ALTER USER $user_name WITH PASSWORD '$password';
            END IF;
        END
        \$\$;
        ALTER DATABASE $db_name OWNER TO $user_name;
EOSQL
}

setup_db() {
    local db_name=$1
    local user_name=$2
    local password=$3
    local description=$4
    
    if [ "$POSTGRES_DB" != "$db_name" ]; then
        create_database_if_not_exists "$db_name" "$description"
    else
        echo "$description database $db_name already exists (same as POSTGRES_DB), skipping creation"
    fi
    create_user_and_grant_permissions "$db_name" "$user_name" "$password" "$description"
}

echo "Initializing Postgres databases..."

# Get environment variables with defaults
GRAFANA_DB=${GRAFANA_POSTGRES_DB:-grafana}
GRAFANA_USER=${GRAFANA_POSTGRES_USER:-grafana}
GRAFANA_PASSWORD=${GRAFANA_POSTGRES_PASSWORD}

setup_db "$GRAFANA_DB" "$GRAFANA_USER" "$GRAFANA_PASSWORD" "Grafana"

echo "✓ Postgres initialization complete"
echo "  - Grafana database: $GRAFANA_DB (user: $GRAFANA_USER)"

