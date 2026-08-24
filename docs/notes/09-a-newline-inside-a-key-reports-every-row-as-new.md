# A newline inside a key reports every row as new, forever

The "what changed since last run" report is a set difference: snapshot the
master's `job_key` column before a rebuild, diff the next build against it. The
obvious file format for a set of keys is one key per line.

That format is **silently wrong**. Tier-3 keys — `company|title|url`, used when
a row has no parseable ATS URL — carry newlines straight out of the source
title, because several sources publish multi-line titles. Those keys split
across two lines, never match anything on the next run, and report themselves as
new on every subsequent run. Nothing errors; the diff just quietly stops being a
diff.

## The mechanism

[extract_baseline_keys.py](../../extract_baseline_keys.py) writes the snapshot
as JSON-escaped keys, one per line — `json.dumps(k)` turns an embedded newline
into `\n`, so one key is always exactly one line, and the reader recovers it
with `json.loads`.

The part worth copying is the self-check, because it catches the whole class of
bug rather than the one instance:

> a baseline file can never hold more lines than the master holds rows.

If it does, the escaping did not happen and the baseline is unusable, so the
script exits non-zero rather than writing a file that would poison every future
diff. A second assertion checks that the line count equals the number of keys
written.

## Two ordering constraints

The snapshot has to be taken **before** any rebuild. The master is opened with
`"w"`, there is no `.prev` copy and no rotation anywhere in the pipeline, so the
moment a refresh reaches its build tail the previous key set is gone and the
diff has nothing to compare against.

The script also validates the master's header before reading a fixed column
index, since a reordered or renamed column would snapshot the wrong column
without any other symptom. The header check tolerates a master that merely
predates an *appended* optional column — which is why every optional column in
the schema is appended at the end and nowhere else.
