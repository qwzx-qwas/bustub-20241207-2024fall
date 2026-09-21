"""Shared nonempty fixture loading and independent whole-table verification."""

from content import Operation, Oracle, fixture, insert_rows, read_range, read_point, save_fixture
from runtime import write_json


def verify_all(adapter, oracle, count, phase, deadline, **read_options):
    for low in range(0, count, 128):
        operation = read_range(low, min(low + 128, count))
        oracle.check(operation, adapter.read(operation, phase, deadline, **read_options))
    # Sentinel ranges also detect unexpected out-of-domain inserts.
    for operation in (read_range(-(2**31), 0), read_range(count, 2**31 - 1), read_point(2**31 - 1)):
        oracle.check(operation, adapter.read(operation, phase, deadline, **read_options))


def prepare(adapter, root, config, control):
    rows = fixture(config["rows"], config["seed"])
    write_json(root / "fixture.json", save_fixture(root / "fixture.jsonl", rows))
    oracle = Oracle()
    operation = Operation("CreateSchema", {})
    adapter.write(operation, 1, "schema", control.deadline)
    oracle.apply(operation)
    for offset in range(0, len(rows), 64):
        operation = insert_rows(rows[offset:offset + 64])
        adapter.write(operation, 2, "load", control.deadline)
        oracle.apply(operation)
    verify_all(adapter, oracle, len(rows), "verify_load", control.deadline)
    return rows, oracle
