#!/bin/bash
# Template: Authenticated Session Workflow
# Purpose: Login once, save state, reuse for subsequent runs
# Usage: ./authenticated-session.sh <login-url> [state-file]
#
# RECOMMENDED: Use the auth vault instead of this template:
#   echo "<pass>" | agent-browser auth save myapp --url <login-url> --username <user> --password-stdin
#   agent-browser auth login myapp
# The auth vault stores credentials securely and the LLM never sees passwords.
#
# Environment variables:
#   AUTH_PROFILE - Encrypted agent-browser auth-vault profile name
#   XDG_STATE_HOME - Optional root for saved browser state
#
# Two modes:
#   1. Discovery mode (default): Shows the login page and vault setup steps
#   2. Login mode: Uses AUTH_PROFILE without exposing the password to processes
#
# Setup steps:
#   1. Save credentials with `agent-browser auth save --password-stdin`.
#   2. Set AUTH_PROFILE to that encrypted profile name.
#   3. Run this template to log in and persist reusable browser state.

set -euo pipefail
umask 077

LOGIN_URL="${1:?Usage: $0 <login-url> [state-file]}"
if [[ -n "${2:-}" ]]; then
    STATE_FILE="$2"
else
    : "${HOME:?HOME must be set when no state-file is provided}"
    STATE_DIRECTORY="${XDG_STATE_HOME:-$HOME/.local/state}/trifetch-agent-browser"
    mkdir -p "$STATE_DIRECTORY"
    chmod 700 "$STATE_DIRECTORY"
    STATE_FILE="$STATE_DIRECTORY/auth-state.json"
fi

echo "Authentication workflow: $LOGIN_URL"

# ================================================================
# SAVED STATE: Skip login if valid saved state exists
# ================================================================
if [[ -f "$STATE_FILE" ]]; then
    echo "Loading saved state from $STATE_FILE..."
    if agent-browser state load "$STATE_FILE" &&
        agent-browser open "$LOGIN_URL" &&
        agent-browser wait --load networkidle; then
        CURRENT_URL=$(agent-browser get url)
        if [[ "$CURRENT_URL" != *"login"* ]] && [[ "$CURRENT_URL" != *"signin"* ]]; then
            echo "Session restored successfully"
            agent-browser snapshot -i
            exit 0
        fi
        echo "Session expired, performing fresh login..."
    else
        echo "State restoration failed; preserving $STATE_FILE and re-authenticating..." >&2
    fi
    agent-browser close 2>/dev/null || true
fi

# ================================================================
# AUTH VAULT LOGIN: Avoid putting passwords in process arguments
# ================================================================
if [[ -n "${AUTH_PROFILE:-}" ]]; then
    echo "Logging in with encrypted auth profile: $AUTH_PROFILE"
    agent-browser auth login "$AUTH_PROFILE"
    agent-browser open "$LOGIN_URL"
    agent-browser wait --load networkidle

    FINAL_URL=$(agent-browser get url)
    if [[ "$FINAL_URL" == *"login"* ]] || [[ "$FINAL_URL" == *"signin"* ]]; then
        EVIDENCE_DIRECTORY=$(mktemp -d "${TMPDIR:-/tmp}/trifetch-auth.XXXXXX")
        LOGIN_FAILURE_SCREENSHOT="$EVIDENCE_DIRECTORY/login-failed.png"
        agent-browser screenshot "$LOGIN_FAILURE_SCREENSHOT"
        echo "Login failed; screenshot saved to $LOGIN_FAILURE_SCREENSHOT" >&2
        agent-browser close
        exit 1
    fi

    mkdir -p "$(dirname "$STATE_FILE")"
    agent-browser state save "$STATE_FILE"
    chmod 600 "$STATE_FILE"
    echo "Login successful; state saved to $STATE_FILE"
    agent-browser snapshot -i
    exit 0
fi

# ================================================================
# DISCOVERY MODE: Shows form structure (delete after setup)
# ================================================================
echo "Opening login page..."
agent-browser open "$LOGIN_URL"
agent-browser wait --load networkidle

echo ""
echo "Login form structure:"
echo "---"
agent-browser snapshot -i
echo "---"
echo ""
echo "Next steps:"
echo "  1. Save credentials to the encrypted auth vault without a password argument:"
echo "     read -rsp 'Password: ' AUTH_PASSWORD"
echo "     printf '%s' \"\$AUTH_PASSWORD\" | agent-browser auth save myapp --url '$LOGIN_URL' --username '<user>' --password-stdin"
echo "     unset AUTH_PASSWORD"
echo "  2. Run: AUTH_PROFILE=myapp $0 '$LOGIN_URL' '$STATE_FILE'"
echo "  3. Delete this DISCOVERY MODE section after setup"
echo ""
agent-browser close
exit 0
