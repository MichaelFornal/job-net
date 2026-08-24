# A throttle penalty with no decay is permanent

Raising the request rate **halved** throughput.

The shared client in [notion_common.py](../../notion_common.py) talks to one
host for every tenant, so it carries one global throttle. Four workers at
2.0 rps produced 6× HTTP 429 over 518 s, and each 429 pauses globally and widens
the request interval. Starting the sweep at 3.0 rps instead of 1.5 tripped
enough 429s early to pin the interval at its 1.0 s floor, and the sweep then ran
at **21 candidates/min — half the 39/min a conservative 1.5 rps start sustained
over the same host** for the rest of the run.

## Why "back off harder" is the wrong instinct

The penalty was already correct in isolation: on a 429, pause 20 s and multiply
the interval by 1.5, capped at a 1.0 s floor. The defect is that it only ever
*ratchets*. With no path back, one unlucky burst in the first minutes of a
multi-hour sweep sets the rate for the entire run, and the aggressive start
guarantees that burst. Backing off harder makes the trap close faster.

## The mechanism

Both halves have to exist. `_penalise()` widens the interval; `_reward()` walks
it back toward the configured base after `DECAY_AFTER = 150` clean requests, one
step at a time and never below the base. That turns the throttle into a control
loop that finds the host's real ceiling instead of a latch that records the
worst moment of the run.

The operational reading is the one worth keeping: on a long sweep the starting
rate is not a throughput knob, it is a bet about how early the first 429 lands.
A conservative start with decay beat an aggressive start without it by 2× on the
same host, same workers, same endpoint.

The same module documents the discovery constraints that shaped the rest of the
client — a bare tenant root returns a ~19,148-byte JS shell with no page id for
every tenant, live or dead, so discovery has to yield full paths rather than a
subdomain list.
