#!/bin/bash
set -euo pipefail

# Ensure Homebrew PostgreSQL 16 binaries are in PATH
export PATH="/opt/homebrew/opt/postgresql@16/bin:$PATH"

###############################
#         Constants           #
###############################
ENV_FILE=".env"
ADMIN_USER="$USER"  # macOS Homebrew Postgres runs as your user
DEFAULT_DB="postgres"  # Default database to connect to initially

###############################
#      Argument Parsing       #
###############################
FORCE=false
if [[ "${1:-}" == "--force" ]]; then
  FORCE=true
  echo "🚨 Force flag enabled: Existing database and user will be dropped."
fi

###############################
#        Environment          #
###############################
if [[ ! -f "$ENV_FILE" ]]; then
  echo "❌ .env file not found! Exiting..."
  exit 1
fi

# Extract DATABASE_URL from .env
DATABASE_URL=""
while IFS='=' read -r key value; do
  [[ -z "$key" ]] && continue
  [[ "$key" =~ ^# ]] && continue
  if [[ "$key" == "DATABASE_URL" ]]; then
    DATABASE_URL=$(echo "$value" | tr -d '\r')
    break
  fi
done < "$ENV_FILE"

if [[ -z "$DATABASE_URL" ]]; then
  echo "❌ DATABASE_URL not found in .env. Exiting..."
  exit 1
fi

# Parse DATABASE_URL: postgresql://user:password@host:port/dbname
proto_removed="${DATABASE_URL#*://}"
PGUSER="${proto_removed%%:*}"
rest="${proto_removed#*:}"
PGPASSWORD="${rest%%@*}"
rest="${rest#*@}"
PGHOST="${rest%%:*}"
rest="${rest#*:}"
PGPORT="${rest%%/*}"
PGDATABASE="${rest#*/}"

for var in PGHOST PGDATABASE PGUSER PGPASSWORD PGPORT; do
  if [[ -z "${!var:-}" ]]; then
    echo "❌ Could not parse $var from DATABASE_URL. Exiting..."
    exit 1
  fi
done

echo "🔎 Loaded from DATABASE_URL:"
echo "    PGHOST=$PGHOST"
echo "    PGDATABASE=$PGDATABASE"
echo "    PGUSER=$PGUSER"
echo "    PGPORT=$PGPORT"

echo "🧑‍💻 Using admin user: $ADMIN_USER for setup."

###############################
#        Helper Functions     #
###############################

check_postgres_installed() {
  brew list postgresql@16 &>/dev/null
}

uninstall_postgres() {
  echo "🧹 Uninstalling PostgreSQL..."
  brew services stop postgresql@16 &>/dev/null || true
  brew uninstall postgresql@16 --force || true
  rm -rf /opt/homebrew/var/postgresql@16
  echo "✔ PostgreSQL uninstalled."
}

wait_for_postgres() {
  echo "⏳ Waiting for PostgreSQL to be ready..."
  for i in {1..30}; do
    if psql -U "$ADMIN_USER" -d "$DEFAULT_DB" -c "\q" 2>/dev/null; then
      echo "✅ PostgreSQL is ready!"
      return 0
    fi
    echo "⏳ Waiting for PostgreSQL to be ready... ($i/30)"
    sleep 1
  done
  echo "❌ PostgreSQL failed to start within 30 seconds"
  return 1
}

database_exists() {
  psql -U "$ADMIN_USER" -d "$DEFAULT_DB" -Atqc "SELECT 1 FROM pg_database WHERE datname='$PGDATABASE'" | grep -q 1
}

user_exists() {
  psql -U "$ADMIN_USER" -d "$DEFAULT_DB" -Atqc "SELECT 1 FROM pg_roles WHERE rolname='$PGUSER'" | grep -q 1
}

install_postgres() {
  echo "⬇️  Installing PostgreSQL..."
  brew update
  brew install postgresql@16
  echo "✔ PostgreSQL installed successfully."
}

grant_privileges() {
  echo "🔑 Granting privileges to user '$PGUSER' on database '$PGDATABASE'..."
  psql -U "$ADMIN_USER" -d "$DEFAULT_DB" -c "GRANT ALL PRIVILEGES ON DATABASE $PGDATABASE TO $PGUSER;"
  psql -U "$ADMIN_USER" -d "$PGDATABASE" -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO $PGUSER;"
  psql -U "$ADMIN_USER" -d "$PGDATABASE" -c "GRANT USAGE, CREATE ON SCHEMA public TO $PGUSER;"
  echo "✔ Privileges granted."
}

force_remove_database_and_user() {
  echo "🚨 Dropping existing database and user (if they exist)..."
  if database_exists; then
    echo "Terminating active connections to '$PGDATABASE'..."
    psql -U "$ADMIN_USER" -d "$DEFAULT_DB" -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$PGDATABASE' AND pid <> pg_backend_pid();"
    psql -U "$ADMIN_USER" -d "$DEFAULT_DB" -c "DROP DATABASE IF EXISTS $PGDATABASE;" || { echo "❌ Failed to drop database $PGDATABASE"; exit 1; }
    echo "✔ Database '$PGDATABASE' dropped."
  else
    echo "ℹ️  Database '$PGDATABASE' does not exist."
  fi
  if user_exists; then
    psql -U "$ADMIN_USER" -d "$DEFAULT_DB" -c "DROP USER IF EXISTS $PGUSER;" || { echo "❌ Failed to drop user $PGUSER"; exit 1; }
    echo "✔ User '$PGUSER' dropped."
  else
    echo "ℹ️  User '$PGUSER' does not exist."
  fi
}

create_database_and_user() {
  echo "⚙️  Creating database '$PGDATABASE' and user '$PGUSER'..."
  if ! database_exists; then
    psql -U "$ADMIN_USER" -d "$DEFAULT_DB" -c "CREATE DATABASE $PGDATABASE;"
    echo "✔ Database '$PGDATABASE' created."
    # Wait for the database to be ready
    for i in {1..5}; do
      if psql -U "$ADMIN_USER" -d "$PGDATABASE" -c "\q" 2>/dev/null; then
        break
      fi
      echo "⏳ Waiting for database to be ready... ($i)"; sleep 1
    done
  else
    echo "ℹ️  Database '$PGDATABASE' already exists. Skipping creation."
  fi
  if ! user_exists; then
    psql -U "$ADMIN_USER" -d "$DEFAULT_DB" -c "CREATE USER $PGUSER WITH PASSWORD '$PGPASSWORD';"
    psql -U "$ADMIN_USER" -d "$DEFAULT_DB" -c "ALTER ROLE $PGUSER WITH LOGIN;"
    echo "✔ User '$PGUSER' created."
  else
    echo "ℹ️  User '$PGUSER' already exists. Updating password..."
    psql -U "$ADMIN_USER" -d "$DEFAULT_DB" -c "ALTER USER $PGUSER WITH PASSWORD '$PGPASSWORD';"
  fi
  grant_privileges
}

check_user_privileges() {
  echo "🔍 Checking privileges for user '$PGUSER'..."
  local result
  result=$(psql -U "$ADMIN_USER" -d "$DEFAULT_DB" -tAc "SELECT grantee, privilege_type FROM information_schema.role_table_grants WHERE grantee = '$PGUSER';")
  if [[ -z "$result" ]]; then
    echo "⚠️  User '$PGUSER' does not have the required privileges. Granting now..."
    grant_privileges
  else
    echo "✔ User '$PGUSER' privileges:"
    echo "$result"
  fi
}

test_database_connection() {
  echo "🔄 Testing database connection as $PGUSER..."
  PGPASSWORD="$PGPASSWORD" psql -U "$PGUSER" -d "$PGDATABASE" -c "CREATE TABLE IF NOT EXISTS test_table (id SERIAL PRIMARY KEY, name VARCHAR(50) NOT NULL);"
  PGPASSWORD="$PGPASSWORD" psql -U "$PGUSER" -d "$PGDATABASE" -c "INSERT INTO test_table (name) VALUES ('Test Entry');"
  local result
  result=$(PGPASSWORD="$PGPASSWORD" psql -U "$PGUSER" -d "$PGDATABASE" -tAc "SELECT name FROM test_table WHERE name = 'Test Entry';")
  if [[ "$result" == "Test Entry" ]]; then
    echo "✅ Database test successful! Connection is working."
  else
    echo "❌ Database test failed! Could not insert/select data."
    exit 1
  fi
  PGPASSWORD="$PGPASSWORD" psql -U "$PGUSER" -d "$PGDATABASE" -c "DROP TABLE IF EXISTS test_table;"
  echo "✔ Cleanup complete. Test table removed."
}

###############################
#         Main Flow           #
###############################

if ! command -v brew &> /dev/null; then
    echo "❌ Homebrew is not installed. Please install it first from https://brew.sh"
    exit 1
fi

if check_postgres_installed; then
  uninstall_postgres
fi

install_postgres

echo "🚀 Starting PostgreSQL service..."
brew services start postgresql@16

# Wait for PostgreSQL to be ready
if ! wait_for_postgres; then
  echo "❌ Failed to start PostgreSQL service"
  exit 1
fi

if [[ "$FORCE" == "true" ]]; then
  force_remove_database_and_user
fi

create_database_and_user
check_user_privileges
test_database_connection

echo "🎉 PostgreSQL setup completed successfully!"
echo "---------------------------------------------"
echo "Database: $PGDATABASE"
echo "User:     $PGUSER"
echo "Host:     $PGHOST"
echo "Port:     $PGPORT"
echo "---------------------------------------------"
