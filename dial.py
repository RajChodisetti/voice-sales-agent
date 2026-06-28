"""
dial.py — Convenience script to trigger an outbound call.

Usage:
    python dial.py +61412345678
    python dial.py +61412345678 --campaign camp_01

PUBLIC_BASE_URL is read from .env — no hardcoding.
"""

import sys
import os
import requests
from dotenv import load_dotenv

load_dotenv()


def dial(number: str, campaign_id: str = "default"):
    base_url = os.environ.get("PUBLIC_BASE_URL", "").rstrip("/")
    if not base_url:
        print("ERROR: PUBLIC_BASE_URL is not set in .env")
        sys.exit(1)

    url = f"{base_url}/call"
    print(f"Dialling {number} via {url} (campaign: {campaign_id}) ...")

    headers = {"ngrok-skip-browser-warning": "true"}
    resp = requests.post(url, json={"to": number, "campaign_id": campaign_id}, headers=headers)

    print(f"Status Code: {resp.status_code}")
    try:
        data = resp.json()
        print("Response:", data)
    except Exception:
        print("Failed to parse JSON. Raw response:")
        print("-" * 50)
        print(resp.text)
        print("-" * 50)
        sys.exit(1)

    status = data.get("status")
    if status == "calling":
        print(f"Call initiated! SID: {data['call_sid']}")
        print(f"Waiting for {number} to pick up...")
    elif status == "blocked":
        print(f"Call blocked: {data.get('reason')}")
    elif status == "queued":
        print(f"Call queued: {data.get('reason')} — {data.get('message', '')}")
    else:
        print(f"Unexpected response: {data}")


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        print("Usage: python dial.py +<countrycode><number> [--campaign <id>]")
        print("Example: python dial.py +61412345678")
        sys.exit(1)

    number = args[0]
    campaign = "default"
    if "--campaign" in args:
        idx = args.index("--campaign")
        campaign = args[idx + 1]

    dial(number, campaign)
