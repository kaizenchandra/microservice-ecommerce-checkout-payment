#!/usr/bin/env python3
"""Issue a short-lived local demo JWT. Access to the signing key grants issuer authority."""
import argparse
import base64
import hashlib
import hmac
import json
import os
from pathlib import Path
import time

IDENTITIES = {"customer": ("11111111-1111-1111-1111-111111111111", "CUSTOMER"),
              "other": ("22222222-2222-2222-2222-222222222222", "CUSTOMER"),
              "admin": ("admin", "ADMIN"), "checkout": ("checkout", "CHECKOUT")}


def token(subject, secret, ttl=900, issuer="checkout-demo", audience="ecommerce-api"):
    roles = {identity: role for identity, role in IDENTITIES.values()}
    if subject not in roles or len(secret.encode()) < 32 or not 1 <= ttl <= 3600:
        raise ValueError("A known demo identity, a 32-byte signing key and TTL of 1–3600 seconds are required")
    now = int(time.time())
    encode = lambda value: base64.urlsafe_b64encode(value).rstrip(b"=")
    claims = {"iss": issuer, "aud": [audience], "sub": subject, "roles": [roles[subject]], "iat": now, "nbf": now, "exp": now + ttl}
    signing = b".".join(encode(json.dumps(value, separators=(",", ":")).encode()) for value in ({"alg": "HS256", "typ": "JWT"}, claims))
    return (signing + b"." + encode(hmac.new(secret.encode(), signing, hashlib.sha256).digest())).decode()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--identity", choices=IDENTITIES, default="customer")
    parser.add_argument("--ttl", type=int, default=900)
    args = parser.parse_args()
    path = Path(__file__).resolve().parents[2] / ".env"
    values = dict(line.split("=", 1) for line in path.read_text().splitlines() if line and not line.startswith("#") and "=" in line) if path.exists() else {}
    values.update(os.environ)
    secret = values.get("JWT_SECRET", "")
    try: print(token(IDENTITIES[args.identity][0], secret, args.ttl, values.get("JWT_ISSUER", "checkout-demo"), values.get("JWT_AUDIENCE", "ecommerce-api")))
    except ValueError as error: parser.error(str(error))


if __name__ == "__main__":
    main()
