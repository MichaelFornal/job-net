#!/bin/sh
# Fail the build if anything personal reached the tree or the history.
#
# This is the BACKSTOP, not the control.  The control is the curated export
# allowlist -- this catches what the allowlist missed.  Do not start relying on
# it to filter for you.
#
# Usage: tools/scan_secrets.sh [--no-history]
# Exit:  0 clean (warnings may still print), 1 denylist hit.

set -eu

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"

DENY=tools/denylist.txt
WARN=tools/warnlist.txt
SCAN_HISTORY=1
[ "${1:-}" = "--no-history" ] && SCAN_HISTORY=0

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT INT TERM

strip() { sed -e 's/[[:space:]]*#.*$//' -e '/^[[:space:]]*$/d' "$1"; }
strip "$DENY" > "$TMP/deny"
# Identity-specific patterns live OUTSIDE the published denylist -- writing
# a real name into a public repo to check that no real name is in a public
# repo defeats the exercise.  Present locally, gitignored, optional in CI.
LOCAL=tools/denylist.local.txt
[ -f "$LOCAL" ] && strip "$LOCAL" >> "$TMP/deny" && echo "   + $(strip "$LOCAL" | wc -l | tr -d ' ') local pattern(s)"
strip "$WARN" > "$TMP/warn"
if [ -s tools/allowlist.txt ]; then strip tools/allowlist.txt > "$TMP/allow"; else : > "$TMP/allow"; fi
# An empty pattern file makes grep match everything, so guard it.
sift() { python3 "$ROOT/tools/_sift.py" "$TMP/deny" "$TMP/allow" || true; }

# The list files are the one place these strings may legally appear.
EXCL='^\(tools/denylist\.txt\|tools/warnlist\.txt\|tools/allowlist\.txt\|tools/denylist\.local\.txt\|tools/scan_secrets\.sh\)$'

files() {
  if [ -d .git ]; then git ls-files; else find . -type f | sed 's|^\./||'; fi \
    | grep -v "$EXCL" || true
}

rc=0

# A malformed pattern makes grep abort and print nothing, which reads exactly
# like a clean scan.  That failure mode already happened once: a '#' inside a
# character group was eaten by comment stripping, the group was left unbalanced,
# and the gate reported PASS over a planted credential.  Validate first.
badpat=0
while IFS= read -r pat; do
  # -e, because a pattern beginning with "-" (a PEM header, say) is read as
  # a flag otherwise and reports itself uncompilable.
  if printf '' | grep -Eq -e "$pat" 2>/dev/null; [ $? -gt 1 ]; then
    echo "FAIL: uncompilable pattern: $pat" >&2
    badpat=1
  fi
done < "$TMP/deny"
# The sift step reads these same patterns with Python's re, which does not
# understand POSIX classes.  A pattern that means one thing to grep and another
# to Python is how a real credential slips through the allowlist, so both
# engines have to accept it.
if ! python3 "$ROOT/tools/_sift.py" --check "$TMP/deny"; then
  badpat=1
fi

if [ "$badpat" = 1 ]; then
  echo "RESULT: FAIL (the scanner itself is broken; fix the patterns)"
  exit 1
fi

# --- 1. working tree ------------------------------------------------------
echo "== scanning working tree =="
files > "$TMP/files"
if [ -s "$TMP/files" ]; then
  : > "$TMP/hits"
  : > "$TMP/err"
  # Per file, so the allowlist can exempt individual LINES.  Filtering whole
  # files instead would let one documented-public string carry a real secret in
  # the same file straight past the gate.
  while IFS= read -r f; do
    set +e
    grep -IEni -f "$TMP/deny" "$f" 2>>"$TMP/err" | sift > "$TMP/lines"
    set -e
    if [ -s "$TMP/lines" ]; then
      echo "$f" >> "$TMP/hits"
    fi
  done < "$TMP/files"

  if [ -s "$TMP/err" ]; then
    echo "FAIL: the scan itself errored -- treating that as a failure, not a pass:"
    sed 's/^/      /' "$TMP/err"
    rc=1
  fi

  if [ -s "$TMP/hits" ]; then
    echo "FAIL: denylisted content in the working tree:"
    while IFS= read -r f; do
      echo "  --- $f"
      grep -IEni -f "$TMP/deny" "$f" 2>/dev/null | sift | head -5 | sed 's/^/      /'
    done < "$TMP/hits"
    rc=1
  else
    echo "  clean ($(wc -l < "$TMP/files" | tr -d ' ') files)"
  fi
else
  echo "  no files yet"
fi

# EXCL hides the identity list from the content scan (it would match itself),
# so being tracked needs a check of its own.  It names real people and places;
# it reached a public push once, when a refresh copied a .gitignore without it.
if [ -d .git ] && git ls-files --error-unmatch "$LOCAL" >/dev/null 2>&1; then
  echo "FAIL: $LOCAL is tracked -- git rm --cached it and keep it in .gitignore"
  rc=1
fi

# --- 2. history -----------------------------------------------------------
if [ "$SCAN_HISTORY" = 1 ] && [ -d .git ] && git rev-parse HEAD >/dev/null 2>&1; then
  echo "== scanning git history =="
  if git log -p --format= --all -- . ':(exclude)tools/denylist.txt' \
     ':(exclude)tools/warnlist.txt' ':(exclude)tools/scan_secrets.sh' \
     2>/dev/null | grep -IEni -f "$TMP/deny" | sift > "$TMP/hhits"; [ -s "$TMP/hhits" ]; then
    echo "FAIL: denylisted content in git history ($(wc -l < "$TMP/hhits" | tr -d ' ') lines):"
    head -10 "$TMP/hhits" | sed 's/^/      /'
    echo "      (history cannot be edited in place -- rebuild the repo)"
    rc=1
  else
    echo "  clean ($(git rev-list --count --all) commits)"
  fi

  echo "== checking commit authorship =="
  git log --format='%ae%n%ce' --all | sort -u > "$TMP/emails"
  if grep -qvE 'users\.noreply\.github\.com|^noreply@' "$TMP/emails"; then
    echo "FAIL: non-noreply address baked into commit metadata:"
    grep -vE 'users\.noreply\.github\.com|^noreply@' "$TMP/emails" | sed 's/^/      /'
    rc=1
  else
    echo "  clean ($(wc -l < "$TMP/emails" | tr -d ' ') address(es), all noreply)"
  fi
fi

# --- 3. warnings (never fail) --------------------------------------------
echo "== prose check (warnings only) =="
if [ -s "$TMP/files" ] && <"$TMP/files" xargs grep -IEnil -f "$TMP/warn" 2>/dev/null > "$TMP/whits"; then
  echo "  $(wc -l < "$TMP/whits" | tr -d ' ') file(s) contain operator-specific prose:"
  sed 's/^/      /' "$TMP/whits"
  echo "  -> reword to 'the operator'; the measured numbers are worth keeping."
else
  echo "  clean"
fi

echo
[ "$rc" = 0 ] && echo "RESULT: PASS" || echo "RESULT: FAIL"
exit $rc
