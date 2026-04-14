#!/usr/bin/env python3
"""
Bootstraps Kestra: uploads/updates all flows from ./flows, then triggers
the 01_gcp_kv flow to populate the KV store.

Usage:
    source .env && python entrypoint.py

Requires: pip install requests python-dotenv pyyaml
"""

import json
import os
import sys
import time
import yaml
import requests
from dotenv import load_dotenv

load_dotenv()

KESTRA_URL = "http://localhost:8080/api/v1"
NAMESPACE   = "fsa_livestock"
AUTH        = ("admin@kestra.io", "Admin1234!")
FLOWS_DIR   = "./flows"
ADC_PATH    = os.path.expanduser("~/.config/gcloud/application_default_credentials.json")

KV = {
    "GCP_PROJECT_ID":     os.getenv("TF_VAR_project"),
    "GCP_LOCATION":       os.getenv("TF_VAR_location"),
    "GCP_BUCKET_NAME":    os.getenv("TF_VAR_gcs_bucket_name"),
    "GCP_DATASET":        os.getenv("TF_VAR_bq_dataset_name"),
    "GCP_SA_IMPERSONATE": os.getenv("GCP_SA_IMPERSONATE"),
}


def load_adc():
    if not os.path.exists(ADC_PATH):
        print(f"WARNING: ADC file not found at {ADC_PATH}")
        print("  Run `gcloud auth application-default login` first.")
        return None
    with open(ADC_PATH, "r") as f:
        return json.dumps(json.load(f))


def wait_for_kestra(retries=12, delay=5):
    print("Waiting for Kestra to be ready...")
    for i in range(retries):
        try:
            r = requests.get(f"{KESTRA_URL}/flows/search", auth=AUTH, timeout=5)
            if r.status_code == 200:
                print("Kestra is ready.\n")
                return
        except requests.exceptions.ConnectionError:
            pass
        print(f"  Not ready yet, retrying in {delay}s... ({i+1}/{retries})")
        time.sleep(delay)
    print("ERROR: Kestra did not become ready in time.")
    sys.exit(1)


def upload_flow(flow_path):
    with open(flow_path, "r") as f:
        content = f.read()

    parsed = yaml.safe_load(content)
    flow_id   = parsed["id"]
    namespace = parsed["namespace"]
    headers   = {"Content-Type": "application/x-yaml"}

    # Try POST (create); on conflict (409 or 422), fall back to PUT (update)
    r = requests.post(f"{KESTRA_URL}/flows", data=content, headers=headers, auth=AUTH)
    if r.status_code in (409, 422):
        r = requests.put(
            f"{KESTRA_URL}/flows/{namespace}/{flow_id}",
            data=content, headers=headers, auth=AUTH,
        )

    status = "OK" if r.status_code in (200, 201) else f"ERROR {r.status_code}: {r.text}"
    print(f"  {os.path.basename(flow_path)}: {status}")


def upload_flows():
    yml_files = sorted(f for f in os.listdir(FLOWS_DIR) if f.endswith(".yml"))
    if not yml_files:
        print("  No .yml files found in ./flows")
        return
    for filename in yml_files:
        upload_flow(os.path.join(FLOWS_DIR, filename))


def trigger_kv_flow(adc_content):
    """Trigger 01_gcp_kv with .env values + ADC content as inputs and wait for completion."""
    print(f"\nTriggering '{NAMESPACE}/01_gcp_kv' to populate KV store...")
    params = {
        "inputs.gcp_project_id":     KV["GCP_PROJECT_ID"],
        "inputs.gcp_location":       KV["GCP_LOCATION"],
        "inputs.gcp_bucket_name":    KV["GCP_BUCKET_NAME"],
        "inputs.gcp_dataset":        KV["GCP_DATASET"],
        "inputs.gcp_sa_impersonate": KV["GCP_SA_IMPERSONATE"],
    }
    if adc_content:
        params["inputs.gcp_adc"] = adc_content
    r = requests.post(
        f"{KESTRA_URL}/executions/{NAMESPACE}/01_gcp_kv",
        params=params,
        auth=AUTH,
    )
    if r.status_code not in (200, 201):
        print(f"  ERROR triggering flow: {r.status_code} - {r.text}")
        return

    execution_id = r.json()["id"]
    print(f"  Execution started: {execution_id}")

    # Poll until terminal state
    for _ in range(30):
        time.sleep(3)
        r = requests.get(f"{KESTRA_URL}/executions/{execution_id}", auth=AUTH)
        state = r.json().get("state", {}).get("current", "UNKNOWN")
        if state in ("SUCCESS", "FAILED", "KILLED", "WARNING"):
            print(f"  KV flow finished: {state}")
            return
        print(f"  State: {state}...")

    print("  WARNING: timed out waiting for KV flow to finish — check Kestra UI")


def main():
    missing = [k for k, v in KV.items() if not v]
    if missing:
        print(f"ERROR: missing env vars for: {missing}")
        print("Did you run `source .env` before this script?")
        sys.exit(1)

    adc_content = load_adc()

    wait_for_kestra()

    print("Uploading flows from ./flows:")
    upload_flows()

    trigger_kv_flow(adc_content)

    print("\nDone. Trigger the pipeline from the Kestra UI at http://localhost:8080")


if __name__ == "__main__":
    main()
