#!/usr/bin/env python
# coding: utf-8

import argparse
import os
import requests


def download_if_available(url, output_dir="."):
    """Check if URL exists and download it. Returns filename if successful, None otherwise."""
    try:
        response = requests.head(url, allow_redirects=True, timeout=10)
        if response.status_code == 200:
            filename = url.split("/")[-1]
            filepath = os.path.join(output_dir, filename)
            r = requests.get(url, timeout=30)
            with open(filepath, "wb") as f:
                f.write(r.content)
            print(f"  Downloaded: {filename} ({len(r.content) // 1024}K)", flush=True)
            return filename
    except Exception as e:
        print(f"  Error fetching {url}: {e}", flush=True)
    return None


def download_all(output_dir="."):
    os.makedirs(output_dir, exist_ok=True)

    quarter_months = {
        1: ("jan", "mar"),
        2: ("apr", "jun"),
        3: ("jul", "sep"),
        4: ("oct", "dec"),
    }

    special_cases = {
        ("poultry", 17, 1): "poultry-conditions-jul16-mar17.csv",
    }

    datasets = {
        "poultry": "https://fsadata.github.io/poultry-conditions/data",
        "pig":     "https://fsadata.github.io/pig-conditions/data",
    }

    for dataset, base_url in datasets.items():
        print(f"\n=== {dataset.upper()} ===", flush=True)
        for year in range(17, 21):
            for quarter in range(1, 5):
                q_init, q_fin = quarter_months[quarter]

                key = (dataset, year, quarter)
                if key in special_cases:
                    url = f"{base_url}/{special_cases[key]}"
                    if not download_if_available(url, output_dir):
                        print(f"  Not found (special case): {special_cases[key]}", flush=True)
                    continue

                # Try both naming conventions (with/without dash) and "sept" variant
                q_fin_variants = [q_fin, q_fin.replace("sep", "sept")]
                candidates = (
                    [f"{base_url}/{dataset}-conditions-{q_init}{year}-{qf}{year}.csv"   for qf in q_fin_variants] +
                    [f"{base_url}/{dataset}-conditions-{q_init}-{year}-{qf}-{year}.csv" for qf in q_fin_variants]
                )

                downloaded = any(download_if_available(url, output_dir) for url in candidates)
                if not downloaded:
                    print(f"  Not found: {dataset} 20{year} Q{quarter} ({q_init}-{q_fin})", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download poultry and pig condition data.")
    parser.add_argument(
        "--output-dir",
        default=os.environ.get("OUTPUT_DIR", "./data"),
        help="Directory to save downloaded files (default: $OUTPUT_DIR or ./data)",
    )
    args = parser.parse_args()

    print(f"Saving files to: {args.output_dir}")
    download_all(args.output_dir)
    print("\nDone.")
