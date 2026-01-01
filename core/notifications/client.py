import os
import requests
import json
from typing import Optional

class NotificationClient:
    """
    Handles sending alerts to external platforms (Slack/Discord) via Webhooks.
    """
    def __init__(self, webhook_url: Optional[str] = None):
        self.webhook_url = webhook_url or os.environ.get("NOTIFICATION_WEBHOOK_URL")

    def send(self, message: str) -> bool:
        """Sends a raw message to the webhook."""
        if not self.webhook_url:
            print(f"🔕 Notification (Mock): {message}")
            return False

        try:
            # Detect Slack vs Discord based on URL structure or just send generic JSON
            payload = {"text": message}
            if "discord" in self.webhook_url:
                payload = {"content": message}
                
            response = requests.post(
                self.webhook_url,
                data=json.dumps(payload),
                headers={'Content-Type': 'application/json'}
            )
            if response.status_code in [200, 204]:
                return True
            else:
                print(f"Failed to send notification: {response.text}")
                return False
        except Exception as e:
            print(f"Error sending notification: {e}")
            return False

    def send_report_alert(self, report, filename: str):
        """Sends a formatted alert for a new Earnings Report."""
        
        # Summarize guidance
        guidance_text = ""
        if report.guidance:
            g = report.guidance[0]
            guidance_text = f" | Guide: {g.metric} {g.midpoint}"
            
        msg = (
            f"🚨 **New Earnings Note Generated!**\n"
            f"**{report.company_name} ({report.ticker})** - {report.fiscal_period}\n"
            f"📄 Report saved: `{filename}`\n"
            f"{guidance_text}"
        )
        return self.send(msg)
