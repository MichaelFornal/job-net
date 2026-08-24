#!/usr/bin/env python3
"""Filter denylist hits through the allowlist, substring-wise.

Exempting a whole LINE because part of it is documented-public is a hole: a real
credential sitting on the same line rides out on the exemption. So instead of
asking "is this line allowed", remove every allowlisted substring and ask
whether what is LEFT still matches the denylist. If it does, the line is a real
hit and the exemption does not apply to it.

    grep ... | python3 tools/_sift.py <deny-file> <allow-file>
"""
import re
import sys


def load(path):
    out = []
    with open(path, encoding="utf-8", errors="replace") as fh:
        for raw in fh:
            line = re.sub(r"\s*#.*$", "", raw).strip()
            if line:
                out.append(line)
    return out


def check(path):
    """Every pattern must compile cleanly under Python too, and mean the same
    thing it means to grep.  A FutureWarning here (POSIX class read as a nested
    set) is not cosmetic: it is the two engines disagreeing."""
    import warnings
    bad = 0
    for i, p in enumerate(load(path), 1):
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            try:
                re.compile(p, re.I)
            except Exception as e:
                sys.stderr.write(f"FAIL: pattern {i} is not portable: {p}\n"
                                 f"      {e}\n")
                bad = 1
    return bad


def main():
    if sys.argv[1] == "--check":
        return check(sys.argv[2])
    deny = load(sys.argv[1])
    allow = load(sys.argv[2]) if len(sys.argv) > 2 else []
    dre = [re.compile(p, re.I) for p in deny]
    are = [re.compile(p, re.I) for p in allow]
    for line in sys.stdin:
        s = line.rstrip("\n")
        stripped = s
        for a in are:
            stripped = a.sub("", stripped)
        if any(d.search(stripped) for d in dre):
            sys.stdout.write(s + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
