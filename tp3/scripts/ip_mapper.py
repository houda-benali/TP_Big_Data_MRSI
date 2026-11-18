#!/usr/bin/env python3
import sys

for line in sys.stdin:
    fields = line.split()
    if fields:
        print(f"{fields[0]}\t1")
