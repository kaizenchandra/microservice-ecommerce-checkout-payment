#!/usr/bin/env python3
"""Repeatable infrastructure smoke checks. Requires the running Compose stack."""
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[2]
os.chdir(ROOT)


def compose(*args, data=None, success=True):
    result = subprocess.run(['docker', 'compose', *args], input=data, text=True,
                            capture_output=True, timeout=90)
    if success and result.returncode:
        raise AssertionError(f'Compose operation {args[:3]} failed: {result.stderr[-1500:]}')
    return result


def get(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)


# Never print or pass passwords on the host command line.
for domain in ['product', 'cart', 'checkout', 'order', 'inventory', 'payment', 'shipping', 'notification', 'query']:
    var = domain.upper() + '_DB_PASSWORD'
    script = f'export PGPASSWORD="${var}"; exec psql -h 127.0.0.1 -U {domain}_owner -d {domain}_db -v ON_ERROR_STOP=1 -At'
    result = compose('exec', '-T', 'postgres', 'sh', '-c', script,
                     data="BEGIN; CREATE TABLE public.infrastructure_probe(id int PRIMARY KEY); INSERT INTO public.infrastructure_probe VALUES (1); SELECT count(*) FROM public.infrastructure_probe; ROLLBACK;\n")
    assert '\n1\n' in result.stdout, domain
    other = 'cart' if domain != 'cart' else 'product'
    denied = compose('exec', '-T', 'postgres', 'sh', '-c',
                     f'export PGPASSWORD="${var}"; exec psql -h 127.0.0.1 -U {domain}_owner -d {other}_db -Atc "SELECT 1"', success=False)
    assert denied.returncode != 0 and 'permission denied for database' in denied.stderr, domain
print('PostgreSQL: all 9 owners can transact in their database; cross-database connections denied')

compose('exec', '-T', 'redis', 'sh', '-c', 'export REDISCLI_AUTH="$REDIS_PASSWORD"; test "$(redis-cli ping)" = PONG')
print('Redis: authenticated PING passed')

for topic in ['order.events', 'inventory.events', 'payment.events', 'shipping.events', 'notification.events']:
    result = compose('exec', '-T', 'kafka', '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'kafka:19092', '--describe', '--topic', topic)
    assert 'PartitionCount: 3' in result.stdout and 'ReplicationFactor: 1' in result.stdout, topic
message = 'phase2-' + str(uuid.uuid4())
compose('exec', '-T', 'kafka', '/opt/kafka/bin/kafka-console-producer.sh', '--bootstrap-server', 'kafka:19092',
        '--topic', 'infrastructure.smoke', '--producer-property', 'acks=all', '--property', 'parse.key=true',
        '--property', 'key.separator=:', data=message + ':' + message + '\n')
result = compose('exec', '-T', 'kafka', '/opt/kafka/bin/kafka-console-consumer.sh', '--bootstrap-server', 'kafka:19092',
                 '--topic', 'infrastructure.smoke', '--from-beginning', '--timeout-ms', '5000', success=False)
assert message in result.stdout, 'Kafka record not received'
print('Kafka: 3-partition topics and acknowledged publish/consume passed')

# Read only the optional port overrides from .env; process environment takes precedence.
settings = {}
for line in (ROOT / '.env').read_text().splitlines():
    if line and not line.startswith('#') and '=' in line:
        key, value = line.split('=', 1)
        if key.endswith('_PORT'):
            settings[key] = value
settings.update({k: v for k, v in os.environ.items() if k.endswith('_PORT')})
def port(key, default):
    return settings.get(key, str(default))

assert get('http://localhost:' + port('GRAFANA_PORT', 3000) + '/api/health')['database'] == 'ok'
assert get('http://localhost:' + port('KAFKA_UI_PORT', 8090) + '/actuator/health')['status'] == 'UP'
prom = 'http://localhost:' + port('PROMETHEUS_PORT', 9090)
assert get(prom + '/api/v1/status/config')['status'] == 'success'
compose('exec', '-T', 'prometheus', 'promtool', 'check', 'config', '/etc/prometheus/prometheus.yml')
compose('exec', '-T', 'otel-collector', '/otelcol-contrib', 'validate', '--config=/etc/otelcol/config.yml')
print('Grafana, Kafka UI, Prometheus and collector configuration: passed')

trace_id = uuid.uuid4().hex
now = time.time_ns()
payload = {'resourceSpans': [{'resource': {'attributes': [{'key': 'service.name', 'value': {'stringValue': 'infrastructure-smoke'}}]},
    'scopeSpans': [{'scope': {'name': 'phase2'}, 'spans': [{'traceId': trace_id, 'spanId': uuid.uuid4().hex[:16],
    'name': 'infrastructure-verification', 'kind': 1, 'startTimeUnixNano': str(now), 'endTimeUnixNano': str(now + 1000000)}]}]}]}
request = urllib.request.Request('http://localhost:' + port('OTLP_HTTP_PORT', 4318) + '/v1/traces',
                                 data=json.dumps(payload).encode(), headers={'Content-Type': 'application/json'})
with urllib.request.urlopen(request, timeout=5) as response:
    assert response.status == 200
    reply = json.load(response)
    assert int(reply.get('partialSuccess', {}).get('rejectedSpans', 0)) == 0
# Query Tempo through Grafana's provisioned datasource only when authenticated is cumbersome;
# use the existing Prometheus container's HTTP client on the internal network.
for attempt in range(20):
    result = compose('exec', '-T', 'prometheus', 'wget', '-qO-', f'http://tempo:3200/api/traces/{trace_id}', success=False)
    if result.returncode == 0 and 'infrastructure-verification' in result.stdout:
        break
    time.sleep(1)
else:
    raise AssertionError('OTLP span did not become queryable in Tempo')
print('Tracing: synthetic OTLP span traversed collector and was retrieved from Tempo')

if '--apps' in os.sys.argv:
    for attempt in range(12):
        targets = get(prom + '/api/v1/targets')['data']['activeTargets']
        apps = [t for t in targets if t['labels'].get('job') == 'applications']
        if len(apps) == 10 and all(t['health'] == 'up' for t in apps):
            break
        time.sleep(5)
    else:
        raise AssertionError('Expected 10 healthy application scrape targets')
    print('Applications: all 10 Prometheus scrape targets are UP')
print('Phase 2 verification passed')
