#!/bin/sh
# Reorder wrapper for the codex app-server (tasks #55/#52, reuse ChatGPT Pro).
# OpenClaw 2026.5.19 appends --dangerously-bypass-approvals-and-sandbox AFTER the
# `app-server` subcommand, but codex 0.133 wants it as a GLOBAL flag (before the
# subcommand) → "unexpected argument" otherwise. We pull the flag out and place
# it first. Bound over the image's wrapper via docker-compose; pointed to by
# OPENCLAW_CODEX_APP_SERVER_BIN. Native fix expected in OpenClaw >=2026.5.20.
flag=""
rest=""
for a in "$@"; do
  if [ "$a" = "--dangerously-bypass-approvals-and-sandbox" ]; then flag="$a"; else rest="$rest \"$a\""; fi
done
eval "exec /usr/local/bin/codex $flag $rest"
