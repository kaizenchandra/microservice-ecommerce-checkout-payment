#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
docker compose config --quiet
bash -n infrastructure/postgres/01-databases.sh infrastructure/kafka/topics.sh
python3 - <<'PY'
import ast, json
from pathlib import Path
for script in Path('infrastructure/scripts').glob('*.py'):
    ast.parse(script.read_text())
json.loads(Path('infrastructure/grafana/dashboards/platform.json').read_text())
print('Compose model, shell/Python syntax and dashboard JSON validated')
PY
