#!/usr/bin/env python3
"""Durably truncate the newest immutable snapshot while its node is stopped."""

import argparse
import os
import pathlib


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("snapshot_directory", type=pathlib.Path)
    arguments = parser.parse_args()
    # pathlib does not support a fixed-width digit glob; filter formal names explicitly.
    candidates = sorted(
        path
        for path in arguments.snapshot_directory.glob("SNAPSHOT-*")
        if path.is_file() and len(path.name) == len("SNAPSHOT-") + 20 and path.name[9:].isdigit()
    )
    if len(candidates) < 2:
        raise RuntimeError("latest-snapshot corruption requires two retained generations")
    target = candidates[-1]
    with target.open("r+b", buffering=0) as snapshot:
        size = target.stat().st_size
        if size < 64:
            raise RuntimeError("snapshot is too small to corrupt safely")
        # A short formal header is independently and unambiguously invalid;
        # recovery cannot accidentally accept it through a matching checksum
        # implementation or a payload region that the test never observes.
        os.ftruncate(snapshot.fileno(), 16)
        os.fdatasync(snapshot.fileno())
    directory = os.open(arguments.snapshot_directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    print(target)


if __name__ == "__main__":
    main()
