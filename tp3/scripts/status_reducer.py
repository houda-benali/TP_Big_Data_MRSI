#!/usr/bin/env python3
import sys

current = None
total = 0

for line in sys.stdin:
    line = line.rstrip("\n")
    if not line:
        continue
    parts = line.split("\t", 1)
    if len(parts) != 2:
        continue
    key, value = parts
    try:
        value = int(value)
    except ValueError:
        continue

    if current is not None and key != current:
        print(f"{current}\t{total}")
        total = 0

    current = key
    total += value

if current is not None:
    print(f"{current}\t{total}")
