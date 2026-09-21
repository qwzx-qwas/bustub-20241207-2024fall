"""Versioned business content and an independent oracle; no production imports.

YCSB's separation of values and access distributions informs this module.
These are synthetic fixtures, not a claim about a measured customer workload.
"""

from dataclasses import asdict, dataclass, replace
import hashlib
import json
import random


CONTENT_VERSION = "t0-a-1"
C1_WORKLOAD_VERSION = "c1-resize-1"
PERFORMANCE_VERSION = "t0-c-2"
SEED = 20260920
COLUMNS = ("id", "tenant_id", "kind", "version", "last_op", "payload")
TEMPLATES = (
    "order=ready|region=east|priority=normal|channel=web|",
    "config=active|region=west|retry=enabled|owner=service|",
    "order=paid|region=north|priority=high|channel=mobile|",
    "config=pending|region=south|retry=disabled|owner=batch|",
)


def derived_seed(seed, purpose):
    digest = hashlib.sha256(f"{CONTENT_VERSION}/{seed}/{purpose}".encode()).digest()
    return int.from_bytes(digest[:8], "big")


@dataclass(frozen=True)
class Row:
    id: int
    tenant_id: int
    kind: int
    version: int
    last_op: int
    payload: str

    def values(self):
        return [str(getattr(self, name)) for name in COLUMNS]


@dataclass(frozen=True)
class Operation:
    name: str
    args: dict

    def record(self):
        return {"name": self.name, "args": self.args}


def payload_text(length, template, prefix=""):
    motif = prefix + TEMPLATES[template % len(TEMPLATES)]
    return (motif * ((length + len(motif) - 1) // len(motif)))[:length]


def fixture(count, seed=SEED):
    if count <= 0:
        raise ValueError("fixture must contain records")
    weights = (60, 30, 10)
    counts = [count * weight // 100 for weight in weights]
    order = sorted(range(3), key=lambda i: (-(count * weights[i] % 100), i))
    for index in order[:count - sum(counts)]:
        counts[index] += 1
    lengths = [length for length, amount in zip((64, 256, 1024), counts) for _ in range(amount)]
    rng = random.Random(derived_seed(seed, "fixture"))
    rng.shuffle(lengths)
    keys = list(range(count))
    rng.shuffle(keys)
    shared = set(keys[:count // 2])
    result = []
    for key, length in enumerate(lengths):
        tenant, kind = rng.randrange(32), rng.randrange(4)
        prefix = "" if key in shared else f"record={key}|tenant={tenant}|kind={kind}|"
        result.append(Row(key, tenant, kind, 0, 0, payload_text(length, kind, prefix)))
    return result


def insert_rows(rows):
    return Operation("InsertRows", {"rows": [asdict(row) for row in rows]})


def bump(key, number):
    return Operation("Bump", {"id": key, "last_op": number})


def read_point(key):
    return Operation("ReadPoint", {"id": key})


def read_range(low, high):
    return Operation("ReadRange", {"low": low, "high": high})


def c1_updates(rows):
    for row in rows:
        if row.id % 4 == 0:
            # Change actual byte lengths within the agreed size classes. Keep
            # fixture/P2 seeds unchanged; version this C1 operation separately.
            length = {64: 256, 256: 1024, 1024: 64}[len(row.payload)]
            yield Operation("ReplacePayload", {
                "id": row.id, "last_op": row.id + 1,
                "payload": payload_text(length, row.kind, f"updated={row.id}|"),
            })


def c1_deletes(rows):
    for row in rows:
        if row.id % 4 == 1:
            yield Operation("Delete", {"id": row.id})


def c1_reinserts(rows):
    for row in rows:
        if row.id % 8 == 1:
            yield replace(row, version=2, last_op=100000 + row.id,
                          payload=payload_text(len(row.payload), row.kind, f"reinserted={row.id}|"))


def p2_operations(count, concurrency, worker, seed):
    """Infinite fixed stream, independent of completions and server results."""
    low, high = count * worker // concurrency, count * (worker + 1) // concurrency
    if low == high:
        raise ValueError("P2 client must own a nonempty key range")
    rng = random.Random(derived_seed(seed, f"P2/{concurrency}/{worker}"))
    sequence = 0
    while True:
        sequence += 1
        # Disjoint operation numbers across clients and preprocessing.
        number = 1000000 + sequence * concurrency + worker
        if number >= 2**63:
            raise OverflowError("logical operation number exhausted")
        yield bump(rng.randrange(low, high), number)


def p1_operations(count, worker, seed, distribution, phase):
    """A fixed hot range is 5% of keys; 80% of draws target it, 20% the rest."""
    if distribution not in ("uniform", "hotspot") or count < 20:
        raise ValueError("P1 requires a known distribution and at least 20 rows")
    rng = random.Random(derived_seed(seed, f"{PERFORMANCE_VERSION}/P1/{worker}/{distribution}/{phase}"))
    hot = count // 20
    while True:
        if distribution == "uniform":
            key = rng.randrange(count)
        else:
            key = rng.randrange(hot) if rng.randrange(10) < 8 else rng.randrange(hot, count)
        yield read_point(key)


def p3_groups(rows, seed, rounds=10):
    """Twenty mutations per group, constant rows/bytes at every group boundary.

    This state is advanced from prescribed inputs only. A failed operation ends
    the scenario; it never causes the generator to select replacement work.
    """
    if len(rows) < 100 or len(rows) % 20 or rounds <= 0:
        raise ValueError("P3 requires at least 100 rows, a multiple of 20, and positive rounds")
    state = {row.id: row for row in rows}
    buckets = {size: [row.id for row in rows if len(row.payload) == size] for size in (64, 256, 1024)}
    rng = random.Random(derived_seed(seed, f"{PERFORMANCE_VERSION}/P3"))
    number = 0

    def take(size):
        keys = buckets[size]
        index = rng.randrange(len(keys))
        keys[index], keys[-1] = keys[-1], keys[index]
        return state[keys.pop()]

    for round_number in range(1, rounds + 1):
        for _ in range(len(rows) // 20):
            selected, operations = [], []
            for _ in range(7):
                sizes = rng.sample([size for size, keys in buckets.items() if keys], 2)
                left, right = (take(size) for size in sizes)
                for old, size in ((left, sizes[1]), (right, sizes[0])):
                    number += 1
                    new = replace(old, version=old.version + 1, last_op=number,
                                  payload=payload_text(size, old.kind, f"turnover={number}|id={old.id}|"))
                    operations.append(Operation("ReplacePayload", {"id": old.id, "last_op": number,
                                                                     "payload": new.payload}))
                    selected.append(new)
            deleted = [take(rng.choice([size for size, keys in buckets.items() if keys])) for _ in range(3)]
            for old in deleted:
                number += 1
                operations.append(Operation("Delete", {"id": old.id}))
            for old in deleted:
                number += 1
                new = replace(old, version=old.version + 1, last_op=number,
                              payload=payload_text(len(old.payload), old.kind, f"reinsert={number}|id={old.id}|"))
                operations.append(insert_rows([new]))
                selected.append(new)
            for new in selected:
                state[new.id] = new
                buckets[len(new.payload)].append(new.id)
            yield round_number, operations


def p4_operations(count, seed):
    """Fixed 8-read/2-write blocks; writers own disjoint key ranges."""
    if count < 2:
        raise ValueError("P4 writers require nonempty disjoint ranges")
    rng = random.Random(derived_seed(seed, f"{PERFORMANCE_VERSION}/P4"))
    plan_id = 0
    while True:
        for slot in range(10):
            plan_id += 1
            if slot < 8:
                yield plan_id, None, read_point(rng.randrange(count))
            else:
                writer = slot - 8
                low, high = count * writer // 2, count * (writer + 1) // 2
                yield plan_id, writer, bump(rng.randrange(low, high), 1000000 + plan_id)


class Oracle:
    """Expected state comes only from known inputs and confirmed operations."""

    def __init__(self):
        self.rows = {}

    def apply(self, operation):
        args = operation.args
        if operation.name == "CreateSchema":
            return
        if operation.name == "InsertRows":
            for value in args["rows"]:
                row = Row(**value)
                if row.id in self.rows:
                    raise AssertionError(f"content inserts an existing key: {row.id}")
                self.rows[row.id] = row
            return
        key = args["id"]
        if key not in self.rows:
            raise AssertionError(f"content modifies a missing key: {key}")
        if operation.name == "Delete":
            del self.rows[key]
        elif operation.name in ("Bump", "ReplacePayload"):
            old = self.rows[key]
            self.rows[key] = replace(old, version=old.version + 1, last_op=args["last_op"],
                                     payload=args.get("payload", old.payload))
        else:
            raise ValueError(f"not a modelled mutation: {operation.name}")

    def check(self, operation, actual):
        if operation.name == "ReadPoint":
            key = operation.args["id"]
            expected = [self.rows[key].values()] if key in self.rows else []
        elif operation.name == "ReadRange":
            low, high = operation.args["low"], operation.args["high"]
            expected = [self.rows[key].values() for key in sorted(self.rows) if low <= key < high]
        else:
            raise ValueError("oracle check requires a read")
        # Compare order, duplicates, every field and missing records. Never deduplicate actual.
        if actual != expected:
            mismatch = next((i for i, pair in enumerate(zip(expected, actual)) if pair[0] != pair[1]),
                            min(len(expected), len(actual)))
            raise AssertionError(f"{operation.record()} differs at row {mismatch}: "
                                 f"expected {expected[mismatch:mismatch + 1]!r}, "
                                 f"actual {actual[mismatch:mismatch + 1]!r}; "
                                 f"row counts {len(expected)} vs {len(actual)}")


def save_fixture(path, rows):
    digest = hashlib.sha256()
    with path.open("x", encoding="utf-8") as output:
        for row in rows:
            line = json.dumps(asdict(row), sort_keys=True, separators=(",", ":")) + "\n"
            digest.update(line.encode())
            output.write(line)
    return {"content_version": CONTENT_VERSION, "sha256": digest.hexdigest(), "rows": len(rows),
            "payload_bytes": sum(len(row.payload.encode("ascii")) for row in rows)}


def history_operations(seed, worker):
    """Sixteen alternating operations on one shared key; independent of responses."""
    numbers = list(range(8))
    random.Random(derived_seed(seed, f"history/{worker}")).shuffle(numbers)
    for number in numbers:
        yield bump(7, 100000 + worker * 8 + number)
        yield read_point(7)
