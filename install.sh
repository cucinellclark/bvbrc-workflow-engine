#!/usr/bin/env bash
# =============================================================================
# install.sh - Installation script for the BV-BRC Workflow Engine
# =============================================================================
#
# Usage:
#   ./install.sh              # Full install (venv + deps + log dirs + verify)
#   ./install.sh --deps-only  # Only install/update Python dependencies
#   ./install.sh --help       # Show this help message
#
# Prerequisites:
#   - Python 3.11+
#   - pip
#   - Access to MongoDB instance (configured in config/config.yaml)
#
# Optional:
#   - PM2 + Node.js (for production process management)
#
# =============================================================================

set -euo pipefail

# ---- Constants ----
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${SCRIPT_DIR}/workflow_venv"
REQUIREMENTS_FILE="${SCRIPT_DIR}/requirements.txt"
CONFIG_FILE="${SCRIPT_DIR}/config/config.yaml"
MIN_PYTHON_MAJOR=3
MIN_PYTHON_MINOR=11

# ---- Colors (disabled if not a terminal) ----
if [ -t 1 ]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    BLUE='\033[0;34m'
    NC='\033[0m' # No Color
else
    RED='' GREEN='' YELLOW='' BLUE='' NC=''
fi

# ---- Helpers ----
info()  { echo -e "${BLUE}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }
die()   { error "$*"; exit 1; }

usage() {
    sed -n '/^# Usage:/,/^# ====/p' "$0" | sed 's/^# \?//'
    exit 0
}

# ---- Argument parsing ----
DEPS_ONLY=false
for arg in "$@"; do
    case "$arg" in
        --deps-only) DEPS_ONLY=true ;;
        --help|-h)   usage ;;
        *)           die "Unknown option: $arg (try --help)" ;;
    esac
done

# ---- Step 1: Locate Python ----
find_python() {
    # Prefer python3.11, then python3, then python
    for cmd in python3.11 python3 python; do
        if command -v "$cmd" &>/dev/null; then
            echo "$cmd"
            return
        fi
    done
    return 1
}

check_python_version() {
    local python_cmd="$1"
    local version
    version=$("$python_cmd" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>/dev/null) || return 1
    local major minor
    major=$(echo "$version" | cut -d. -f1)
    minor=$(echo "$version" | cut -d. -f2)
    if [ "$major" -lt "$MIN_PYTHON_MAJOR" ] || { [ "$major" -eq "$MIN_PYTHON_MAJOR" ] && [ "$minor" -lt "$MIN_PYTHON_MINOR" ]; }; then
        return 1
    fi
    echo "$version"
}

echo ""
echo "============================================================"
echo "  BV-BRC Workflow Engine - Installer"
echo "============================================================"
echo ""

PYTHON_CMD=$(find_python) || die "Python not found. Please install Python ${MIN_PYTHON_MAJOR}.${MIN_PYTHON_MINOR}+."
PYTHON_VERSION=$(check_python_version "$PYTHON_CMD") || die "Python ${MIN_PYTHON_MAJOR}.${MIN_PYTHON_MINOR}+ required. Found: $($PYTHON_CMD --version 2>&1)."
ok "Found Python ${PYTHON_VERSION} ($(command -v "$PYTHON_CMD"))"

# ---- Step 2: Create virtual environment ----
if [ "$DEPS_ONLY" = true ]; then
    info "Skipping venv creation (--deps-only)"
    if [ ! -d "$VENV_DIR" ]; then
        die "Virtual environment not found at ${VENV_DIR}. Run without --deps-only first."
    fi
else
    if [ -d "$VENV_DIR" ]; then
        info "Virtual environment already exists at ${VENV_DIR}"
    else
        info "Creating virtual environment at ${VENV_DIR}..."
        "$PYTHON_CMD" -m venv "$VENV_DIR" || die "Failed to create virtual environment. Ensure the 'venv' module is available (python3-venv package on Debian/Ubuntu)."
        ok "Virtual environment created"
    fi
fi

# ---- Step 3: Activate virtual environment ----
# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate" || die "Failed to activate virtual environment"
ok "Activated virtual environment"

# ---- Step 4: Upgrade pip ----
info "Upgrading pip..."
pip install --upgrade pip --quiet || die "Failed to upgrade pip"
ok "pip is up to date ($(pip --version | awk '{print $2}'))"

# ---- Step 5: Install Python dependencies ----
if [ ! -f "$REQUIREMENTS_FILE" ]; then
    die "requirements.txt not found at ${REQUIREMENTS_FILE}"
fi

info "Installing Python dependencies from requirements.txt..."
pip install -r "$REQUIREMENTS_FILE" --quiet || die "Failed to install dependencies"
ok "All Python dependencies installed"

if [ "$DEPS_ONLY" = true ]; then
    echo ""
    ok "Dependencies updated successfully."
    exit 0
fi

# ---- Step 6: Create log directories ----
info "Creating log directories..."
mkdir -p "${SCRIPT_DIR}/logs/workflows"
mkdir -p "${SCRIPT_DIR}/logs/pm2"
mkdir -p "${SCRIPT_DIR}/logs/scheduler"
ok "Log directories ready"

# ---- Step 7: Ensure scripts are executable ----
info "Setting script permissions..."
chmod +x "${SCRIPT_DIR}/scripts/"*.sh 2>/dev/null || true
chmod +x "${SCRIPT_DIR}/install.sh" 2>/dev/null || true
ok "Scripts are executable"

# ---- Step 8: Verify configuration ----
if [ -f "$CONFIG_FILE" ]; then
    ok "Configuration file found at ${CONFIG_FILE}"
else
    warn "Configuration file not found at ${CONFIG_FILE}"
    warn "Copy the example config or create one before starting the application."
fi

# ---- Step 9: Verify core imports ----
info "Verifying Python imports..."
IMPORT_CHECK=$(python -c "
import sys
errors = []
for mod in ['fastapi', 'uvicorn', 'pymongo', 'networkx', 'apscheduler', 'prometheus_client', 'structlog', 'yaml', 'pydantic', 'requests']:
    try:
        __import__(mod)
    except ImportError as e:
        errors.append(str(e))
if errors:
    print('FAILED: ' + '; '.join(errors))
    sys.exit(1)
else:
    print('OK')
" 2>&1) || true

if [[ "$IMPORT_CHECK" == "OK" ]]; then
    ok "All core Python packages import successfully"
else
    warn "Some imports failed: ${IMPORT_CHECK}"
    warn "The application may not start correctly."
fi

# ---- Step 10: Check optional dependencies ----
if command -v pm2 &>/dev/null; then
    ok "PM2 found (optional, for production process management)"
else
    info "PM2 not found (optional). Install via: npm install -g pm2"
fi

if command -v mongosh &>/dev/null || command -v mongo &>/dev/null; then
    ok "MongoDB client tools found"
else
    info "MongoDB client tools not found (optional, for debugging)"
fi

# ---- Summary ----
echo ""
echo "============================================================"
echo "  Installation complete"
echo "============================================================"
echo ""
echo "  Project directory:  ${SCRIPT_DIR}"
echo "  Virtual env:        ${VENV_DIR}"
echo "  Config file:        ${CONFIG_FILE}"
echo ""
echo "  To start the application:"
echo ""
echo "    # Activate the virtual environment"
echo "    source ${VENV_DIR}/bin/activate"
echo ""
echo "    # Start both API server and executor (development)"
echo "    ./scripts/start_all.sh"
echo ""
echo "    # Or start individually:"
echo "    ./scripts/start_api.sh"
echo "    ./scripts/start_executor.sh"
echo ""
echo "    # Or use PM2 (production):"
echo "    pm2 start ecosystem.config.js"
echo ""
echo "    # Stop all processes"
echo "    ./scripts/stop_all.sh"
echo ""
echo "  Configuration:"
echo "    Edit config/config.yaml to set MongoDB host, API host/port, etc."
echo "    Environment variables (MONGODB_HOST, API_HOST, API_PORT, etc.)"
echo "    can also be used to override config values."
echo ""
echo "============================================================"
