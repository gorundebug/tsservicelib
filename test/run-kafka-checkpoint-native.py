"""Run TypeScript checkpoint/commit checks on a disposable Kafka; no shared restarts."""
import argparse
from pathlib import Path
import subprocess
import uuid


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--artifacts', required=True)
    parser.add_argument('--test-name-pattern', help='Limit reruns after an isolated fixture correction')
    args = parser.parse_args()
    root = Path(args.artifacts).resolve()
    root.mkdir(parents=True, exist_ok=True)
    sdk = Path(__file__).resolve().parents[1]
    broker = 'sa-ts-checkpoint-' + uuid.uuid4().hex
    settings = {
        'KAFKA_NODE_ID': '1', 'KAFKA_PROCESS_ROLES': 'broker,controller',
        'KAFKA_LISTENERS': 'BROKER://:9092,CONTROLLER://:9093',
        'KAFKA_ADVERTISED_LISTENERS': 'BROKER://127.0.0.1:9092',
        'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP': 'BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT',
        'KAFKA_INTER_BROKER_LISTENER_NAME': 'BROKER',
        'KAFKA_CONTROLLER_LISTENER_NAMES': 'CONTROLLER',
        'KAFKA_CONTROLLER_QUORUM_VOTERS': '1@127.0.0.1:9093',
        'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR': '1',
        'KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS': '0',
    }
    command = ['docker', 'run', '-d', '--name', broker]
    for key, value in settings.items():
        command += ['-e', key + '=' + value]
    try:
        subprocess.run(command + ['apache/kafka:4.1.2'], check=True, capture_output=True)
        command = [
            'docker', 'run', '--rm', '--network', 'container:' + broker,
            '-v', str(sdk / 'dist') + ':/workspace/dist:ro',
            '-v', str(sdk / 'dist-test') + ':/workspace/dist-test:ro',
            '-e', 'SA_TEST_KAFKA_BROKERS=127.0.0.1:9092', '--entrypoint', 'node',
            'tsservicelib-api-check:latest', '--test',
        ]
        if args.test_name_pattern:
            command += ['--test-name-pattern', args.test_name_pattern]
        command += ['dist-test/test/kafka-checkpoint.test.js']
        with (root / 'acceptance.log').open('w') as output:
            result = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, timeout=240)
        if result.returncode:
            raise RuntimeError(f'Kafka failure gate failed: {root}/acceptance.log')
        print(f'PASS TypeScript Kafka checkpoint gate: {root}')
    finally:
        log = subprocess.run(['docker', 'logs', broker], capture_output=True, text=True)
        (root / 'broker.log').write_text(log.stdout + log.stderr)
        subprocess.run(['docker', 'rm', '-f', broker], capture_output=True)


if __name__ == '__main__':
    main()
