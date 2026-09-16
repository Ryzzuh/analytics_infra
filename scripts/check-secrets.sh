#!/usr/bin/env bash
# Refuse to let plaintext secrets reach a commit (SPEC.md §13).
#
# Runs in CI and as a pre-commit hook. The check is deliberately dumb: any secrets/*.yaml that
# is not sops-encrypted fails. A clever check that tries to judge whether the contents "look
# secret" is a check that eventually guesses wrong.
set -euo pipefail

failed=0
shopt -s nullglob

for file in secrets/*.yaml secrets/*.yml; do
  case "$file" in
    *.example.yaml | *.example.yml) continue ;;
  esac

  if ! grep -q '^sops:' "$file" && ! grep -q '"sops"' "$file"; then
    echo "ERROR: $file is not sops-encrypted" >&2
    failed=1
  fi
done

if [[ $failed -eq 0 ]]; then
  echo "secrets: all files encrypted"
fi
exit $failed
