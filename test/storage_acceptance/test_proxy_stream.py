"""Real framed streams and per-message fault rules; no database workload."""
import json
from pathlib import Path
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from importlib.util import module_from_spec, spec_from_file_location

PATH = Path(__file__).resolve().parents[1] / 'support' / 'raft_message_proxy.py'
spec = spec_from_file_location('raft_proxy', PATH)
proxy = module_from_spec(spec)
spec.loader.exec_module(proxy)


def vote(term):
    body = struct.pack('>QQQQ', term, 1, 83, 6)
    group = b'storage-acceptance'
    payload = struct.pack('>QQI', 1, 2, len(group)) + group + struct.pack('>II', 1, len(body)) + body
    crc = 0xffffffff
    for byte in payload:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82f63b78 if crc & 1 else 0)
    return b'BRAFT001' + struct.pack('>II', 1, len(payload)) + payload + struct.pack('>I', crc ^ 0xffffffff)


def wait_for(predicate):
    end = time.monotonic() + 5
    while time.monotonic() < end:
        if predicate():
            return
        time.sleep(.01)
    raise AssertionError('proxy stream condition did not complete')


class ProxyStreamTest(unittest.TestCase):
    def test_same_stream_obeys_changed_rules_and_reuses_target(self):
        with tempfile.TemporaryDirectory() as directory, socket.socket() as target, socket.socket() as reserve:
            root = Path(directory)
            target.bind(('127.0.0.1', 0))
            target.listen(2)
            target.settimeout(5)
            reserve.bind(('127.0.0.1', 0))
            port = reserve.getsockname()[1]
            reserve.close()
            received, errors = [], []

            def receive():
                try:
                    stream, _ = target.accept()
                    with stream:
                        stream.settimeout(5)
                        received.append(proxy.read_frame(stream))
                        # The second frame must use the same accepted stream.
                        received.append(proxy.read_frame(stream))
                except Exception as error:
                    errors.append(error)

            worker = threading.Thread(target=receive)
            worker.start()
            with (root / 'process.log').open('w') as log:
                process = subprocess.Popen([sys.executable, str(PATH), '--listen-port', str(port),
                    '--target-port', str(target.getsockname()[1]), '--controls', str(root), '--observe', '--node-id', '2'],
                    stdout=log, stderr=log)
                try:
                    wait_for(lambda: (root / 'ready').exists())

                    def observed(kind, generation):
                        path = root / 'events.jsonl'
                        if not path.exists():
                            return False
                        lines = path.read_text().splitlines()
                        return any(event.get('event') == kind and event.get('generation') == generation
                                   for event in map(json.loads, lines))

                    def rules(generation, drop):
                        temp = root / 'rules.tmp'
                        temp.write_text(json.dumps({'generation': generation, 'drop_all': drop, 'blocked_from': []}))
                        temp.replace(root / 'rules.json')
                        wait_for(lambda: (root / 'ack.json').exists() and
                                 json.loads((root / 'ack.json').read_text())['generation'] == generation)

                    with socket.create_connection(('127.0.0.1', port), timeout=5) as stream:
                        stream.sendall(vote(7))
                        wait_for(lambda: observed('forwarded', 0))
                        rules(1, True)
                        stream.sendall(vote(8))
                        wait_for(lambda: observed('blocked', 1))
                        rules(2, False)
                        stream.sendall(vote(9))
                        wait_for(lambda: observed('forwarded', 2))
                    worker.join(5)
                    self.assertFalse(worker.is_alive())
                    self.assertEqual(errors, [])
                    self.assertEqual(received, [vote(7), vote(9)])
                    self.assertFalse((root / 'last-error').exists())
                finally:
                    process.terminate()
                    process.wait(timeout=5)
                    worker.join(5)


if __name__ == '__main__':
    unittest.main()
