# Alpha Sentinel Dashboard Runbook

## Deployment

Deployments to the Virtual Private Server (VPS) are managed through a restricted SSH key connection that triggers the `/home/clawd/alpha-sentinel/ops.sh` script.

To deploy the current state of the repository to your VPS:

```bash
/usr/bin/ssh -o BatchMode=yes openclaw-vps-ops deploy
```

Wait until the command completes and says the build/recreate and restart steps are finished.

## Verification

After deployment, you can verify the status directly on the VPS via read-only commands:

### Verify Dashboard Infrastructure
```bash
/usr/bin/ssh -o BatchMode=yes openclaw-vps-ops status
```
*Ensure that `sentinel-api`, `sentinel-agent`, and `sentinel-dashboard` all show as "Up".*

### View Agent Logs
To monitor the sentinel agent or API container directly:
```bash
/usr/bin/ssh -o BatchMode=yes openclaw-vps-ops logs-agent
```

## View Dashboard URL

The frontend dashboard serves HTML traffic from port 3000 mapping to Nginx port 80.
Navigate to your VPS IP over HTTP on port 3000.

**URL:** `http://<YOUR_VPS_IP>:3000`

## Troubleshooting (Common Issues)
*   **"Connection Refused" when visiting Port 3000:** Check if Port 3000 is open in the firewall (e.g., `ufw allow 3000` on Ubuntu) or double-check the `status` command to ensure `sentinel-dashboard` is running.
*   **Dashboard shows Loading forever:** Ensure port 8001 is mapped successfully from `sentinel-api`. Also ensure that the `fetchApi` path matches `/api/`. Nginx routes `/api/` directly to `http://sentinel-api:8001/`.
*   **Missing Data in API request:** Re-verify that the CSV parsing logic successfully runs by observing `logs-agent`.
*   **Stale Dashboard:** Clear your browser cache or try Hard Refresh (`Cmd+Shift+R`) to force redownloading frontend assets.
