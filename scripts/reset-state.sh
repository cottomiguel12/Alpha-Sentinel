#!/usr/bin/env bash
set -euo pipefail

echo "🧨 Deleting /data/agent_state.json inside container..."
ssh openclaw-vps "docker exec alpha-sentinel-sentinel-agent-1 rm -f /data/agent_state.json"

echo "🔄 Restarting agent..."
ssh openclaw-vps "cd /opt/alpha-sentinel && docker compose restart sentinel-agent"

echo "📄 Showing logs..."
ssh openclaw-vps "cd /opt/alpha-sentinel && docker compose logs --tail=40 sentinel-agent"
