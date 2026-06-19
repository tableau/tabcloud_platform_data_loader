#!/usr/bin/env python3
"""
Quick live probe of GET /api/v1/tenants/{tenantId}/users.

Usage:
    python scripts/probe_users_endpoint.py \
        --api-url https://cloudmanager.tableau.com \
        --pat-secret-ref keyring:tabcloud-test/extractor-pat \
        --pat-name test2

Prints the raw first page (maxResults=2) and exits.
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import requests
from common.secrets import resolve_secret


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", required=True)
    parser.add_argument("--pat-name", required=True)
    parser.add_argument("--pat-secret-ref", default=None)
    parser.add_argument("--pat-secret", default=None)
    parser.add_argument("--max-results", type=int, default=2, help="Page size for probe (default 2)")
    args = parser.parse_args()

    api_url = args.api_url.rstrip("/")
    if args.pat_secret_ref:
        pat_secret = resolve_secret(args.pat_secret_ref)
    elif args.pat_secret:
        pat_secret = args.pat_secret
    else:
        sys.exit("Provide --pat-secret-ref or --pat-secret")

    session = requests.Session()

    # Authenticate
    login_url = f"{api_url}/api/v1/pat/login"
    resp = session.post(login_url, json={"token": pat_secret},
                        headers={"Content-Type": "application/json"})
    resp.raise_for_status()
    login_data = resp.json()
    token = login_data.get("sessionToken")
    tenant_id = login_data.get("tenantId")
    print(f"Authenticated. tenantId={tenant_id}")

    headers = {"Content-Type": "application/json", "x-tableau-session-token": token}

    # Probe: first page, small maxResults
    users_url = f"{api_url}/api/v1/tenants/{tenant_id}/users"
    params = {"maxResults": args.max_results}
    resp = session.get(users_url, headers=headers, params=params)
    resp.raise_for_status()
    data = resp.json()

    print("\n=== Raw response (pretty) ===")
    print(json.dumps(data, indent=2))

    print("\n=== Top-level keys ===")
    print(list(data.keys()))

    users = data.get("users") or data.get("items") or []
    print(f"\n=== Users on this page: {len(users)} ===")
    if users:
        print("\n=== First user - all keys ===")
        print(list(users[0].keys()))
        print("\n=== First user - full record ===")
        print(json.dumps(users[0], indent=2))

    print(f"\n=== pageToken present: {'pageToken' in data} ===")


if __name__ == "__main__":
    main()
