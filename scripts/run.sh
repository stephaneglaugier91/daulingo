#!/usr/bin/env bash
set -euo pipefail

# Directory where this script lives (without changing into it)
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

# Optional: pass --fake to generate fake activity data first
GENERATE_FAKE=0
if [[ "${1:-}" == "--fake" ]]; then
  GENERATE_FAKE=1
fi

run_step() {
  local file="$1"
  echo ""
  echo "=============================="
  echo "▶ Running: ${file}"
  echo "=============================="
  python "$SCRIPT_DIR/$file"
}

# Optionally generate fake activity data
if [[ $GENERATE_FAKE -eq 1 ]]; then
  run_step "_generate_fake_activity_data.py"
fi

# Main pipeline
run_step "0_create_tables.py"
run_step "1_upload_data.py"
run_step "2_compute_user_state_daily.py"
run_step "3_inspect_user_state_daily.py"
run_step "4_compute_retention_rates.py"

echo ""
echo "✅ All steps completed successfully."
