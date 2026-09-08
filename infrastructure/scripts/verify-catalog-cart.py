#!/usr/bin/env python3
"""Exercise Phase 3 through the real gateway. --outage also stops/restarts the catalog."""
import argparse
import base64
import json
import os
from pathlib import Path
import subprocess
import urllib.error
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[2]
os.chdir(ROOT)
parser = argparse.ArgumentParser()
parser.add_argument('--outage', action='store_true', help='Briefly stop and restart product-service to test failure handling')
args = parser.parse_args()
values = dict(line.split('=', 1) for line in (ROOT / '.env').read_text().splitlines()
              if line and not line.startswith('#') and '=' in line)
values.update(os.environ)
base = 'http://localhost:' + values.get('GATEWAY_PORT', '8080')
customer = ('11111111-1111-1111-1111-111111111111', values['DEMO_CUSTOMER_PASSWORD'])
other = ('22222222-2222-2222-2222-222222222222', values['DEMO_SECOND_CUSTOMER_PASSWORD'])
admin = ('admin', values['DEMO_ADMIN_PASSWORD'])


def request(method, path, identity=customer, payload=None, status=200):
    headers = {'Content-Type': 'application/json'}
    if identity:
        headers['Authorization'] = 'Basic ' + base64.b64encode((':'.join(identity)).encode()).decode()
    req = urllib.request.Request(base + path, method=method, headers=headers,
                                 data=None if payload is None else json.dumps(payload).encode())
    try:
        response = urllib.request.urlopen(req, timeout=15)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        text = response.read().decode()
        assert response.status == status, f'{method} {path}: expected {status}, received {response.status}: {text}'
        return json.loads(text) if text else None


request('GET', '/api/products', None, status=401)
products = request('GET', '/api/products?size=2')
assert len(products['items']) == 2 and products['totalElements'] >= 4
sku = 'SMOKE-' + uuid.uuid4().hex[:12].upper()
created = request('POST', '/api/products', admin, {'sku': sku, 'name': 'Phase 3 smoke product',
                  'description': 'Synthetic verification data', 'price': {'amount': '12.30', 'currency': 'USD'}}, status=201)
product_path = '/api/products/' + created['id']
assert str(created['price']['currency']) == 'USD'
cart = request('POST', '/api/carts', status=201)
cart_path = '/api/carts/' + cart['id']
request('GET', cart_path, other, status=404)
command = {'productId': created['id'], 'quantity': 2, 'expectedVersion': 0}
changed = request('POST', cart_path + '/items', payload=command)
assert changed['version'] == 1 and changed['items'][0]['quantity'] == 2
request('POST', cart_path + '/items', payload=command, status=409)
request('POST', cart_path + '/items', payload={**command, 'quantity': 0, 'expectedVersion': 1}, status=400)
if args.outage:
    try:
        subprocess.run(['docker', 'compose', 'stop', 'product-service'], check=True, capture_output=True, timeout=60)
        request('POST', cart_path + '/items', payload={**command, 'expectedVersion': 1}, status=503)
        unchanged = request('GET', cart_path)
        assert unchanged['version'] == 1 and unchanged['items'][0]['quantity'] == 2
    finally:
        subprocess.run(['docker', 'compose', 'up', '-d', '--no-deps', '--wait', '--wait-timeout', '120', 'product-service'],
                       check=True, capture_output=True, timeout=150)
    print('Catalog outage: rejected write without changing cart; catalog recovered')
updated = request('PUT', cart_path + '/items/' + created['id'], payload={'quantity': 3, 'expectedVersion': 1})
assert updated['version'] == 2 and updated['items'][0]['quantity'] == 3
# Soft-deactivate only the product created by this run; seeded products remain untouched.
request('PUT', product_path, admin, {'name': created['name'], 'description': created['description'],
                                   'price': created['price'], 'active': False, 'expectedVersion': 0})
request('POST', cart_path + '/items', payload={**command, 'expectedVersion': 2}, status=409)
removed = request('DELETE', cart_path + '/items/' + created['id'] + '?expectedVersion=2')
assert removed['version'] == 3 and removed['items'] == []
print('Gateway routing, catalog CRUD, authentication, ownership, version conflicts, validation and cart changes passed')
print('Retained synthetic records: one inactive product and one empty customer cart')
