"""
show_calls.py — Pretty-print recent calls & transcripts in your terminal.

Usage:
    python show_calls.py           # Show last 10 calls (summary)
    python show_calls.py --id 3    # Show call #3 with full transcript
    python show_calls.py --all     # Show all calls
"""

import sys
import json
from logger_db import list_calls, get_call_summary


def print_calls(limit=10):
    calls = list_calls(limit)
    if not calls:
        print("No calls logged yet.")
        return
    print(f"\n{'─'*70}")
    print(f"  {'ID':<5} {'To':<16} {'Outcome':<15} {'Started':<22} {'Booking'}")
    print(f"{'─'*70}")
    for c in calls:
        booking = ""
        if c.get("booking"):
            b = json.loads(c["booking"])
            booking = f"{b.get('slot','')} ({b.get('prospect_name','')})"
        print(f"  {c['id']:<5} {c['to_number']:<16} {c['outcome']:<15} {(c['started_at'] or '')[:19]:<22} {booking}")
    print(f"{'─'*70}\n")


def print_transcript(call_id: int):
    summary = get_call_summary(call_id)
    if not summary:
        print(f"No call found with id={call_id}")
        return

    booking = json.loads(summary["booking"]) if summary.get("booking") else None
    print(f"\n{'═'*60}")
    print(f"  Call #{summary['id']}  |  {summary['to_number']}")
    print(f"  Outcome : {summary['outcome']}")
    print(f"  Started : {summary.get('started_at','?')[:19]}")
    print(f"  Ended   : {(summary.get('ended_at') or 'still active')[:19]}")
    if booking:
        print(f"  Booking : {booking.get('slot')} for {booking.get('prospect_name')} ({booking.get('prospect_email','')})")
    print(f"{'─'*60}")
    print("  TRANSCRIPT")
    print(f"{'─'*60}")
    for turn in summary.get("transcript", []):
        role  = turn["role"].upper()
        label = "👤 USER     " if role == "USER" else "🤖 ALEX     "
        print(f"  {label}: {turn['content']}")
    print(f"{'═'*60}\n")


if __name__ == "__main__":
    args = sys.argv[1:]
    if "--id" in args:
        idx = args.index("--id")
        call_id = int(args[idx + 1])
        print_transcript(call_id)
    elif "--all" in args:
        print_calls(limit=1000)
    else:
        print_calls(limit=10)
