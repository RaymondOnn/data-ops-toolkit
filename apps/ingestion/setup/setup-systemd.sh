#!/bin/bash
# setup_orchestrator.sh
# 
# NOTE: This script is intended for Production/Staging Linux environments.
# It configures the Orchestrator to run as a systemd background service.
# For local testing, use 'python -m ingestion start' instead.

APP_NAME="ingestion"
PEX_FILE="/home/ubuntu/app/orchestrator.pex"
SERVICE_FILE="/etc/systemd/system/$APP_NAME.service"

echo "--- Systemd Bootstrap Starting ---"

# 1. Permission Check
chmod +x "$PEX_FILE"

# 2. Idempotent Service Creation
if [ ! -f "$SERVICE_FILE" ]; then
    echo "Creating service definition..."
    sudo bash -c "cat > $SERVICE_FILE" <<EOF
[Unit]
Description=Ingestion Orchestrator
After=network.target

[Service]
Type=notify
NotifyAccess=all
User=$(whoami)
WorkingDirectory=$(dirname "$PEX_FILE")
ExecStart=$PEX_FILE
Restart=always
# Restart the app if it doesn't "ping" for 5 minutes
WatchdogSec=300
# Wait 10 seconds before trying a restart to prevent loop-thrashing
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF
    sudo systemctl daemon-reload
    sudo systemctl enable $APP_NAME
fi

# 3. Start/Restart
sudo systemctl restart $APP_NAME
echo "--- Orchestrator is now Always-On ---"