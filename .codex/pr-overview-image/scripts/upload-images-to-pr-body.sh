#!/usr/bin/env bash
# Upload local PR overview images through GitHub's browser attachment flow and
# embed the resulting user-attachments URLs in the PR body.
#
# Usage:
#   upload-images-to-pr-body.sh [--pr <number>] [--section <heading>] <image>...
#
# Requirements:
#   - gh CLI authenticated for repo/PR metadata and PR body edits
#   - agent-browser CLI
#   - a connected/logged-in GitHub browser session; the script uses
#     `agent-browser --auto-connect`
#
# The script manages a marked block in the PR body:
#   <!-- pr-overview-images:start -->
#   ...
#   <!-- pr-overview-images:end -->

set -euo pipefail

PR=""
SECTION_TITLE="PR overview images"
IMAGES=()

while [ "$#" -gt 0 ]; do
  case "$1" in
    --pr)
      PR="${2:-}"
      shift 2
      ;;
    --section)
      SECTION_TITLE="${2:-}"
      shift 2
      ;;
    -h|--help)
      sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    --)
      shift
      while [ "$#" -gt 0 ]; do
        IMAGES+=("$1")
        shift
      done
      ;;
    -*)
      echo "error: unknown option $1" >&2
      exit 1
      ;;
    *)
      IMAGES+=("$1")
      shift
      ;;
  esac
done

if [ "${#IMAGES[@]}" -eq 0 ]; then
  echo "error: expected at least one image path" >&2
  exit 1
fi

for cmd in gh agent-browser python3; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "error: $cmd not found" >&2
    exit 1
  fi
done
if ! gh auth status >/dev/null 2>&1; then
  echo "error: gh not authenticated. Run 'gh auth login'." >&2
  exit 1
fi

if [ -z "$PR" ]; then
  PR="$(gh pr view --json number -q .number 2>/dev/null || true)"
fi
if [ -z "$PR" ]; then
  echo "error: no PR found for current branch. Pass --pr <number>." >&2
  exit 1
fi

PR_URL="$(gh pr view "$PR" --json url -q .url)"
BODY_FILE="$(mktemp "${TMPDIR:-/tmp}/pr-body.XXXXXX")"
SECTION_FILE="$(mktemp "${TMPDIR:-/tmp}/pr-image-section.XXXXXX")"
NEW_BODY_FILE="$(mktemp "${TMPDIR:-/tmp}/pr-body-new.XXXXXX")"
ATTACHMENTS_FILE="$(mktemp "${TMPDIR:-/tmp}/pr-image-attachments.XXXXXX")"

cleanup() {
  rm -f "$BODY_FILE" "$SECTION_FILE" "$NEW_BODY_FILE" "$ATTACHMENTS_FILE"
}
trap cleanup EXIT

ABS_IMAGES=()
for image in "${IMAGES[@]}"; do
  if [ ! -f "$image" ]; then
    echo "error: image not found: $image" >&2
    exit 1
  fi
  case "${image##*.}" in
    png|PNG|jpg|JPG|jpeg|JPEG|gif|GIF|webp|WEBP) ;;
    *)
      echo "error: unsupported image extension: $image" >&2
      exit 1
      ;;
  esac
  ABS_IMAGES+=("$(python3 -c 'import os,sys; print(os.path.abspath(sys.argv[1]))' "$image")")
done

agent-browser --auto-connect open "$PR_URL" >/dev/null
agent-browser --auto-connect wait 2000 >/dev/null

# Clear the new-comment composer. We use it only as GitHub's attachment upload
# surface; no comment is submitted.
agent-browser --auto-connect eval \
  "for (const t of document.querySelectorAll('form.js-new-comment-form textarea, textarea[aria-label=Comment]')) { t.value=''; t.dispatchEvent(new Event('input', {bubbles:true})); } 'cleared'" \
  >/dev/null

for image in "${ABS_IMAGES[@]}"; do
  agent-browser --auto-connect upload 'form.js-new-comment-form input[type=file]' "$image" >/dev/null
done

expected="${#ABS_IMAGES[@]}"
for _ in $(seq 1 30); do
  agent-browser --auto-connect eval \
    "Array.from(document.querySelectorAll('form.js-new-comment-form textarea, textarea[aria-label=Comment]')).map(t=>t.value).find(Boolean) || ''" \
    | python3 -c 'import json,sys; print(json.loads(sys.stdin.read()))' > "$ATTACHMENTS_FILE" || true
  count="$( (grep -o 'github.com/user-attachments/assets/' "$ATTACHMENTS_FILE" || true) | wc -l | tr -d ' ')"
  if [ "$count" -ge "$expected" ]; then
    break
  fi
  sleep 1
done

count="$( (grep -o 'github.com/user-attachments/assets/' "$ATTACHMENTS_FILE" || true) | wc -l | tr -d ' ')"
if [ "$count" -lt "$expected" ]; then
  echo "error: only found $count uploaded attachment URL(s), expected $expected" >&2
  echo "composer content:" >&2
  cat "$ATTACHMENTS_FILE" >&2
  exit 1
fi

# Leave the page clean for humans: keep uploaded URLs, but clear the unsent
# comment composer after we have copied them into the PR body.
agent-browser --auto-connect eval \
  "for (const t of document.querySelectorAll('form.js-new-comment-form textarea, textarea[aria-label=Comment]')) { t.value=''; t.dispatchEvent(new Event('input', {bubbles:true})); } 'cleared'" \
  >/dev/null

{
  echo "<!-- pr-overview-images:start -->"
  echo
  echo "## ${SECTION_TITLE}"
  echo
  cat "$ATTACHMENTS_FILE"
  echo
  echo "<!-- pr-overview-images:end -->"
} > "$SECTION_FILE"

gh pr view "$PR" --json body -q '.body // ""' > "$BODY_FILE"

if grep -q '<!-- pr-overview-images:start -->' "$BODY_FILE" && grep -q '<!-- pr-overview-images:end -->' "$BODY_FILE"; then
  awk -v section_file="$SECTION_FILE" '
    BEGIN {
      while ((getline line < section_file) > 0) {
        section = section line ORS
      }
    }
    $0 == "<!-- pr-overview-images:start -->" {
      printf "%s", section
      in_block = 1
      next
    }
    $0 == "<!-- pr-overview-images:end -->" {
      in_block = 0
      next
    }
    !in_block { print }
  ' "$BODY_FILE" > "$NEW_BODY_FILE"
else
  {
    cat "$BODY_FILE"
    echo
    cat "$SECTION_FILE"
  } > "$NEW_BODY_FILE"
fi

gh pr edit "$PR" --body-file "$NEW_BODY_FILE" >/dev/null

echo "ok|pr=${PR}|attachments=${count}"
