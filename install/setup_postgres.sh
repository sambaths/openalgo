#!/bin/bash
set -euo pipefail

###############################
#         Constants           #
###############################
ENV_FILE=".env"
DEFAULT_DB="postgres"  # Default database to connect to initially

# Platform-specific configurations will be set by detect_platform()
PLATFORM=""
PACKAGE_MANAGER=""
SERVICE_MANAGER=""
POSTGRES_SERVICE=""
POSTGRES_PACKAGE=""
POSTGRES_VERSION="16"
ADMIN_USER=""
POSTGRES_BIN_PATH=""

###############################
#      Argument Parsing       #
###############################
FORCE=false
if [[ "${1:-}" == "--force" ]]; then
  FORCE=true
  echo "🚨 Force flag enabled: Existing database and user will be dropped."
fi

###############################
#    Platform Detection      #
###############################
detect_platform() {
  if [[ "$OSTYPE" == "darwin"* ]]; then
    PLATFORM="macos"
    PACKAGE_MANAGER="brew"
    SERVICE_MANAGER="brew_services"
    POSTGRES_SERVICE="postgresql@16"
    POSTGRES_PACKAGE="postgresql@16"
    ADMIN_USER="$USER"
    POSTGRES_BIN_PATH="/opt/homebrew/opt/postgresql@16/bin"
    # Also check for Intel Mac
    if [[ ! -d "/opt/homebrew" ]] && [[ -d "/usr/local" ]]; then
      POSTGRES_BIN_PATH="/usr/local/opt/postgresql@16/bin"
    fi
  elif [[ -f /etc/debian_version ]]; then
    PLATFORM="debian"
    PACKAGE_MANAGER="apt"
    SERVICE_MANAGER="systemctl"
    POSTGRES_SERVICE="postgresql"
    POSTGRES_PACKAGE="postgresql-16 postgresql-contrib-16"
    ADMIN_USER="postgres"
    POSTGRES_BIN_PATH="/usr/lib/postgresql/16/bin"
  elif [[ -f /etc/redhat-release ]] || [[ -f /etc/fedora-release ]]; then
    PLATFORM="redhat"
    if command -v dnf &> /dev/null; then
      PACKAGE_MANAGER="dnf"
    else
      PACKAGE_MANAGER="yum"
    fi
    SERVICE_MANAGER="systemctl"
    POSTGRES_SERVICE="postgresql-16"
    POSTGRES_PACKAGE="postgresql16-server postgresql16-contrib"
    ADMIN_USER="postgres"
    POSTGRES_BIN_PATH="/usr/pgsql-16/bin"
  elif [[ -f /etc/arch-release ]]; then
    PLATFORM="arch"
    PACKAGE_MANAGER="pacman"
    SERVICE_MANAGER="systemctl"
    POSTGRES_SERVICE="postgresql"
    POSTGRES_PACKAGE="postgresql"
    ADMIN_USER="postgres"
    POSTGRES_BIN_PATH="/usr/bin"
  elif [[ "$OSTYPE" == "freebsd"* ]]; then
    PLATFORM="freebsd"
    PACKAGE_MANAGER="pkg"
    SERVICE_MANAGER="service"
    POSTGRES_SERVICE="postgresql"
    POSTGRES_PACKAGE="postgresql16-server postgresql16-contrib"
    ADMIN_USER="postgres"
    POSTGRES_BIN_PATH="/usr/local/bin"
  elif [[ "$OSTYPE" == "openbsd"* ]]; then
    PLATFORM="openbsd"
    PACKAGE_MANAGER="pkg_add"
    SERVICE_MANAGER="rcctl"
    POSTGRES_SERVICE="postgresql"
    POSTGRES_PACKAGE="postgresql-server"
    ADMIN_USER="postgres"
    POSTGRES_BIN_PATH="/usr/local/bin"
  else
    echo "❌ Unsupported operating system: $OSTYPE"
    echo "Supported platforms: macOS, Ubuntu/Debian, Fedora/CentOS/RHEL, Arch Linux, FreeBSD, OpenBSD"
    exit 1
  fi

  echo "🖥️  Detected platform: $PLATFORM"
  echo "📦 Package manager: $PACKAGE_MANAGER"
  echo "🔧 Service manager: $SERVICE_MANAGER"
}

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

###############################
#    Platform-Specific Funcs #
###############################

check_prerequisites() {
  case $PLATFORM in
    macos)
      if ! command -v brew &> /dev/null; then
        echo "❌ Homebrew is not installed. Please install it first from https://brew.sh"
        exit 1
      fi
      ;;
    debian)
      if ! command -v apt &> /dev/null; then
        echo "❌ APT package manager not found"
        exit 1
      fi
      ;;
    redhat)
      if ! command -v $PACKAGE_MANAGER &> /dev/null; then
        echo "❌ $PACKAGE_MANAGER package manager not found"
        exit 1
      fi
      ;;
    arch)
      if ! command -v pacman &> /dev/null; then
        echo "❌ Pacman package manager not found"
        exit 1
      fi
      ;;
    freebsd|openbsd)
      if ! command -v $PACKAGE_MANAGER &> /dev/null; then
        echo "❌ $PACKAGE_MANAGER package manager not found"
        exit 1
      fi
      ;;
  esac
}

check_postgres_installed() {
  case $PLATFORM in
    macos)
      brew list postgresql@16 &>/dev/null
      ;;
    debian)
      dpkg -l | grep -q postgresql-16
      ;;
    redhat)
      rpm -qa | grep -q postgresql16-server
      ;;
    arch)
      pacman -Qi postgresql &>/dev/null
      ;;
    freebsd)
      pkg info postgresql16-server &>/dev/null
      ;;
    openbsd)
      pkg_info | grep -q postgresql-server
      ;;
  esac
}

install_postgres() {
  echo "⬇️  Installing PostgreSQL..."
  case $PLATFORM in
    macos)
      brew update
      brew install $POSTGRES_PACKAGE
      ;;
    debian)
      # Add PostgreSQL official repository for version 16
      sudo apt update
      sudo apt install -y wget ca-certificates
      wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
      echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main" | sudo tee /etc/apt/sources.list.d/pgdg.list
      sudo apt update
      sudo apt install -y $POSTGRES_PACKAGE
      ;;
    redhat)
      # Install PostgreSQL 16 repository
      sudo $PACKAGE_MANAGER install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-$(rpm -E %{rhel})-x86_64/pgdg-redhat-repo-latest.noarch.rpm
      sudo $PACKAGE_MANAGER install -y $POSTGRES_PACKAGE
      ;;
    arch)
      sudo pacman -Sy --noconfirm $POSTGRES_PACKAGE
      ;;
    freebsd)
      sudo pkg install -y $POSTGRES_PACKAGE
      ;;
    openbsd)
      sudo pkg_add $POSTGRES_PACKAGE
      ;;
  esac
  echo "✔ PostgreSQL installed successfully."
}

uninstall_postgres() {
  echo "🧹 Uninstalling PostgreSQL..."
  stop_postgres_service
  case $PLATFORM in
    macos)
      brew uninstall $POSTGRES_PACKAGE --force || true
      rm -rf /opt/homebrew/var/postgresql@16 || true
      rm -rf /usr/local/var/postgresql@16 || true
      ;;
    debian)
      sudo apt remove --purge -y postgresql-16 postgresql-contrib-16 || true
      sudo rm -rf /var/lib/postgresql/16 || true
      ;;
    redhat)
      sudo $PACKAGE_MANAGER remove -y postgresql16-server postgresql16-contrib || true
      sudo rm -rf /var/lib/pgsql/16 || true
      ;;
    arch)
      sudo pacman -Rns --noconfirm postgresql || true
      sudo rm -rf /var/lib/postgres || true
      ;;
    freebsd)
      sudo pkg delete -y postgresql16-server postgresql16-contrib || true
      sudo rm -rf /usr/local/pgsql || true
      ;;
    openbsd)
      sudo pkg_delete postgresql-server || true
      sudo rm -rf /var/postgresql || true
      ;;
  esac
  echo "✔ PostgreSQL uninstalled."
}

initialize_postgres() {
  case $PLATFORM in
    debian|redhat|arch)
      if [[ ! -d "/var/lib/postgresql/16/main" ]] && [[ ! -d "/var/lib/pgsql/16/data" ]] && [[ ! -d "/var/lib/postgres/data" ]]; then
        echo "🔧 Initializing PostgreSQL database..."
        case $PLATFORM in
          debian)
            sudo -u postgres $POSTGRES_BIN_PATH/initdb -D /var/lib/postgresql/16/main
            ;;
          redhat)
            sudo /usr/pgsql-16/bin/postgresql-16-setup initdb
            ;;
          arch)
            sudo -u postgres initdb -D /var/lib/postgres/data
            ;;
        esac
        echo "✔ PostgreSQL database initialized."
      fi
      ;;
    freebsd)
      if [[ ! -d "/usr/local/pgsql/data" ]]; then
        echo "🔧 Initializing PostgreSQL database..."
        sudo -u postgres /usr/local/bin/initdb -D /usr/local/pgsql/data
        echo "✔ PostgreSQL database initialized."
      fi
      ;;
    openbsd)
      if [[ ! -d "/var/postgresql/data" ]]; then
        echo "🔧 Initializing PostgreSQL database..."
        sudo -u _postgresql initdb -D /var/postgresql/data
        ADMIN_USER="_postgresql"
        echo "✔ PostgreSQL database initialized."
      fi
      ;;
  esac
}

start_postgres_service() {
  echo "🚀 Starting PostgreSQL service..."
  case $SERVICE_MANAGER in
    brew_services)
      brew services start $POSTGRES_SERVICE
      ;;
    systemctl)
      sudo systemctl enable $POSTGRES_SERVICE
      sudo systemctl start $POSTGRES_SERVICE
      ;;
    service)
      sudo service $POSTGRES_SERVICE enable
      sudo service $POSTGRES_SERVICE start
      ;;
    rcctl)
      sudo rcctl enable $POSTGRES_SERVICE
      sudo rcctl start $POSTGRES_SERVICE
      ;;
  esac
}

stop_postgres_service() {
  case $SERVICE_MANAGER in
    brew_services)
      brew services stop $POSTGRES_SERVICE &>/dev/null || true
      ;;
    systemctl)
      sudo systemctl stop $POSTGRES_SERVICE &>/dev/null || true
      sudo systemctl disable $POSTGRES_SERVICE &>/dev/null || true
      ;;
    service)
      sudo service $POSTGRES_SERVICE stop &>/dev/null || true
      ;;
    rcctl)
      sudo rcctl stop $POSTGRES_SERVICE &>/dev/null || true
      sudo rcctl disable $POSTGRES_SERVICE &>/dev/null || true
      ;;
  esac
}

wait_for_postgres() {
  echo "⏳ Waiting for PostgreSQL to be ready..."
  export PATH="$POSTGRES_BIN_PATH:$PATH"
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

# Detect platform and set configurations
detect_platform

echo "🔎 Loaded from DATABASE_URL:"
echo "    PGHOST=$PGHOST"
echo "    PGDATABASE=$PGDATABASE"
echo "    PGUSER=$PGUSER"
echo "    PGPORT=$PGPORT"

echo "🧑‍💻 Using admin user: $ADMIN_USER for setup."

# Check prerequisites
check_prerequisites

# Uninstall existing PostgreSQL if present
if check_postgres_installed; then
  uninstall_postgres
fi

# Install PostgreSQL
install_postgres

# Initialize database (for non-macOS platforms)
initialize_postgres

# Add PostgreSQL binaries to PATH
export PATH="$POSTGRES_BIN_PATH:$PATH"

# Start PostgreSQL service
start_postgres_service

# Wait for PostgreSQL to be ready
if ! wait_for_postgres; then
  echo "❌ Failed to start PostgreSQL service"
  exit 1
fi

# Handle force flag
if [[ "$FORCE" == "true" ]]; then
  force_remove_database_and_user
fi

# Create database and user
create_database_and_user
check_user_privileges
test_database_connection

echo "🎉 PostgreSQL setup completed successfully!"
echo "---------------------------------------------"
echo "Platform: $PLATFORM"
echo "Database: $PGDATABASE"
echo "User:     $PGUSER"
echo "Host:     $PGHOST"
echo "Port:     $PGPORT"
echo "---------------------------------------------"
echo ""
echo "ℹ️  Note: For Windows users, please use WSL (Windows Subsystem for Linux)"
echo "   and run this script within a supported Linux distribution."
