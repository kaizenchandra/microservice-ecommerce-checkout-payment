#!/usr/bin/env python3
"""Verify Phase 4 through the gateway; retain one synthetic append-only order."""
import base64
import json
import os
from pathlib import Path
import time
import urllib.error
import urllib.request
import uuid

root = Path(__file__).resolve().parents[2]
values = dict(line.split('=', 1) for line in (root / '.env').read_text().splitlines()
              if line and not line.startswith('#') and '=' in line)
values.update(os.environ)
base = 'http://localhost:' + values.get('GATEWAY_PORT', '8080')
customer = ('11111111-1111-1111-1111-111111111111', values['DEMO_CUSTOMER_PASSWORD'])
other = ('22222222-2222-2222-2222-222222222222', values['DEMO_SECOND_CUSTOMER_PASSWORD'])
admin = ('admin', values['DEMO_ADMIN_PASSWORD'])
checkout = ('checkout', values['DEMO_CHECKOUT_PASSWORD'])
key, order_id = str(uuid.uuid4()), str(uuid.uuid4())


def request(method, path, identity=customer, payload=None, status=200):
    headers = {'Content-Type': 'application/json', 'Idempotency-Key': key}
    if identity:
        headers['Authorization'] = 'Basic ' + base64.b64encode(':'.join(identity).encode()).decode()
    req = urllib.request.Request(base + path, method=method, headers=headers,
                                 data=None if payload is None else json.dumps(payload).encode())
    try:
        response = urllib.request.urlopen(req, timeout=15)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        if isinstance(status, tuple):
            assert response.status in status, f'{method} {path}: expected {status}, got {response.status}'
            return response.status, json.loads(response.read())
        assert response.status == status, f'{method} {path}: expected {status}, got {response.status}'
        return json.loads(response.read())


command = {
    'orderId': order_id, 'customerId': customer[0], 'cartId': str(uuid.uuid4()), 'cartVersion': 1,
    'items': [{'productId': str(uuid.uuid4()), 'sku': 'DEMO-1', 'name': 'Phase 4 smoke item',
               'quantity': 2, 'unitPrice': {'amount': '12.50', 'currency': 'USD'}}],
    'paymentToken': 'tok_success',
    'shippingAddress': {'recipient': 'Demo Buyer', 'line1': '1 Test Street',
                        'city': 'Test City', 'postalCode': '12345', 'country': 'US'}
}
request('POST', '/api/orders', None, command, 401)
request('POST', '/api/orders', customer, command, 403)
accepted = request('POST', '/api/orders', checkout, command, 202)
assert accepted['orderId'] == order_id and accepted['version'] == 1
assert accepted == request('POST', '/api/orders', checkout, command, 202)
request('POST', '/api/orders', checkout, {**command, 'salesChannel': 'MOBILE'}, 409)
path = '/api/orders/' + order_id
request('GET', path, other, status=404)
request('GET', path + '/events', customer, status=403)
view = request('GET', path)
assert view['status'] in {'PENDING', 'COMPENSATING', 'COMPLETED', 'CANCELLED'} and float(view['total']['amount']) == 25
assert 'paymentToken' not in view
# Saga facts can append concurrently, so refresh the expected version on a 409.
for attempt in range(5):
    view = request('GET', path)
    note = {'expectedVersion': view['version'], 'note': 'Synthetic Phase 4 verification'}
    code, added = request('POST', path + '/notes', payload=note, status=(200, 409))
    if code == 200:
        assert added['version'] == note['expectedVersion'] + 1
        break
else:
    raise AssertionError('Could not append note after five version conflicts')
request('POST', path + '/notes', payload=note, status=409)
deadline = time.monotonic() + 90
while True:
    history = request('GET', path + '/events', admin)
    types = [event['eventType'] for event in history]
    assert types[0] == 'OrderCreated' and types.count('OrderNoteAdded') == 1
    assert [event['aggregateVersion'] for event in history] == list(range(1, len(history) + 1))
    rows = request('GET', path + '/outbox', admin)
    if [row['eventId'] for row in rows] == [event['eventId'] for event in history] and all(row['status'] == 'PUBLISHED' for row in rows):
        break
    assert time.monotonic() < deadline, 'Outbox did not publish within 90 seconds'
    time.sleep(1)
print('Order routing, authorization, idempotency, replay, version conflicts and Kafka acknowledgement passed')
print('Retained synthetic append-only order: ' + order_id)
