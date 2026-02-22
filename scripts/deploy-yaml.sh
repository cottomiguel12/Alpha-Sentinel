#!/usr/bin/env bash
set -euo pipefail

DIR="$HOME/alpha-sentinel"

echo "🔍 Validating YAML..."
"$DIR/scripts/validate-yaml.sh"

echo "📤 Uploading YAML to VPS..."
scp "$DIR/config/agent.yml" openclaw-vps:/opt/alpha-sentinel/config/agent.yml

echo "🔄 Restarting agent..."
ssh openclaw-vps "cd /opt/alpha-sentinel && docker compose restart sentinel-agent"

echo "📄 Showing logs..."
ssh openclaw-vps "cd /opt/alpha-sentinel && docker compose logs --tail=30 sentinel-agent"
