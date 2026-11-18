#!/usr/bin/env python3
import re
import sys

pattern = re.compile(r'"[^"]*"\s+(\d{3})\s+\d+')

for line in sys.stdin:
    match = pattern.search(line)
    if match:
        print(f"{match.group(1)}\t1")
