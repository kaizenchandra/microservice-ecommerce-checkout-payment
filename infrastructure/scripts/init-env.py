#!/usr/bin/env python3
"""Create credentials or append missing template keys; never rotate existing values."""
import os
from pathlib import Path
import secrets

root = Path(__file__).resolve().parents[2]
target = root / ".env"
existing = target.read_text() if target.exists() else ""
keys = {line.split("=", 1)[0] for line in existing.splitlines() if "=" in line and not line.startswith("#")}
missing = []
for line in (root / ".env.example").read_text().splitlines():
    if not line or line.startswith("#") or "=" not in line:
        continue
    key, value = line.split("=", 1)
    if key not in keys:
        missing.append(key + "=" + (value or secrets.token_hex(24)))
if missing:
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    with os.fdopen(fd, "a") as stream:
        stream.write(("\n" if existing and not existing.endswith("\n") else "") + "\n".join(missing) + "\n")
    os.chmod(target, 0o600)
    print(f"Added {len(missing)} missing environment values; existing credentials preserved")
else:
    print(".env already contains all required keys; unchanged")
