"""
Meridiano CRM — Slack integration
"""

import json
import urllib.request
from app.config import SLACK_WEBHOOK_URL


def send_message(text_msg: str) -> bool:
    """Post a message to the configured Slack webhook.  Returns True on success."""
    if not SLACK_WEBHOOK_URL:
        return False
    payload = {"text": text_msg}
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        SLACK_WEBHOOK_URL,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
        return True
    except Exception as e:
        print(f"[slack] error: {e}")
        return False
