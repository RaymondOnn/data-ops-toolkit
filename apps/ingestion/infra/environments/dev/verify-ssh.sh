#!/bin/bash
# scripts/verify_ssh.sh

set -e

USER=$1
HOST=$2
PORT=${3:-22}

if [ -z "$USER" ] || [ -z "$HOST" ]; then
    echo "Usage: $0 <user> <host> [port]"
    exit 1
fi

echo "🧐 Testing SSH connection to $USER@$HOST on port $PORT..."

# -o BatchMode=yes: Prevents hanging on interactive password prompts
# -o ConnectTimeout=5: Limits the wait time for a connection
# -o StrictHostKeyChecking=no: Automatically accepts the host key (typical for mock/dev envs)
if ssh -q -p "$PORT" -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=no "$USER@$HOST" exit; then
    echo "✅ SSH connection verified. Target is ready for deployment."
else
    echo "❌ Failed to connect to $USER@$HOST via SSH."
    echo "Check if the container is running and port $PORT is mapped correctly."
    exit 1
fi
