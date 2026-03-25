#!/usr/bin/env python3
"""Execute SQL statements against Databricks SQL warehouse via REST API."""

import json
import subprocess
import sys
import time

PROFILE = "fe-vm-fevm-serverless-stable-swv01"
WAREHOUSE_ID = "084543d48aafaeb2"


def run_sql(statement: str, label: str = "") -> dict:
    """Execute a SQL statement and return the response."""
    if label:
        print(f">>> {label}")

    payload = json.dumps({
        "warehouse_id": WAREHOUSE_ID,
        "statement": statement,
        "wait_timeout": "50s",
    })

    result = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements",
         f"--profile={PROFILE}", "--json", payload],
        capture_output=True, text=True
    )

    try:
        resp = json.loads(result.stdout)
    except json.JSONDecodeError:
        print(f"    ERROR: {result.stdout[:200]}")
        print(f"    STDERR: {result.stderr[:200]}")
        return {}

    state = resp.get("status", {}).get("state", "UNKNOWN")

    if state == "SUCCEEDED":
        print(f"    OK")
    elif state == "RUNNING" or state == "PENDING":
        # Poll for completion
        stmt_id = resp.get("statement_id", "")
        print(f"    Waiting... (statement_id: {stmt_id})")
        for _ in range(60):
            time.sleep(2)
            poll = subprocess.run(
                ["databricks", "api", "get", f"/api/2.0/sql/statements/{stmt_id}",
                 f"--profile={PROFILE}"],
                capture_output=True, text=True
            )
            try:
                poll_resp = json.loads(poll.stdout)
                poll_state = poll_resp.get("status", {}).get("state", "UNKNOWN")
                if poll_state == "SUCCEEDED":
                    print(f"    OK")
                    return poll_resp
                elif poll_state in ("FAILED", "CANCELED", "CLOSED"):
                    err = poll_resp.get("status", {}).get("error", {}).get("message", "")
                    print(f"    {poll_state}: {err[:300]}")
                    return poll_resp
            except json.JSONDecodeError:
                pass
        print("    TIMEOUT waiting for statement")
    else:
        err = resp.get("status", {}).get("error", {}).get("message", "")
        print(f"    {state}: {err[:300]}")

    return resp


def run_sql_file(filepath: str):
    """Read a SQL file and execute each statement separated by semicolons."""
    with open(filepath, "r") as f:
        content = f.read()

    # Remove SQL comments (-- lines)
    lines = []
    for line in content.split("\n"):
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        lines.append(line)
    content = "\n".join(lines)

    # Split by semicolons (simple approach - doesn't handle strings with semicolons)
    statements = [s.strip() for s in content.split(";") if s.strip()]

    print(f"\n{'='*60}")
    print(f"Executing {filepath} ({len(statements)} statements)")
    print(f"{'='*60}")

    success = 0
    failed = 0
    for i, stmt in enumerate(statements):
        if not stmt or len(stmt) < 5:
            continue
        # Use first line as label
        label = stmt.split("\n")[0][:80]
        run_sql(stmt, f"[{i+1}/{len(statements)}] {label}")
        # Small delay to avoid rate limiting
        time.sleep(0.5)

    return success, failed


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python run_sql.py <sql_file_or_statement> [--statement]")
        sys.exit(1)

    if "--statement" in sys.argv:
        # Direct SQL statement
        stmt = sys.argv[1]
        run_sql(stmt, stmt[:80])
    else:
        # SQL file
        run_sql_file(sys.argv[1])
