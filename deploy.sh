#!/bin/bash
# Deploy script — run this on the SERVER after git pull
# Usage: ./deploy.sh
set -e

echo "🔄 Pulling latest changes..."
git pull origin main

echo "🔍 Checking Python syntax..."
cd engine
for f in *.py; do
    python3 -c "import py_compile; py_compile.compile('$f', doraise=True)" 2>&1 || {
        echo "❌ Syntax error in $f — aborting deploy"
        exit 1
    }
done
cd ..

echo "🔍 Checking JS syntax..."
node --check ui/app.js || {
    echo "❌ JS syntax error — aborting deploy"
    exit 1
}

echo "🔄 Restarting service..."
sudo systemctl restart openclaw-news-analyzer

echo "⏳ Waiting 5 seconds..."
sleep 5

echo "🔍 Checking service status..."
if systemctl is-active --quiet openclaw-news-analyzer; then
    echo "✅ Service running — deploy complete"
    journalctl -u openclaw-news-analyzer --since "10 seconds ago" --no-pager | tail -5
else
    echo "❌ Service failed to start!"
    journalctl -u openclaw-news-analyzer --since "30 seconds ago" --no-pager | tail -20
    exit 1
fi
