#!/usr/bin/env python
# coding: utf-8

import argparse
import os
import requests


QUARTER_MONTHS = {
    1: ("jan", "mar"),
    2: ("apr", "jun"),
    3: ("jul", "sep"),
    4: ("oct", "dec"),
}

SPECIAL_CASES = {
    ("poultry", 17, 1): "poultry-conditions-jul16-mar17.csv",
}

BASE_URLS = {
    "poultry": "https://fsadata.github.io/poultry-conditions/data",
    "pig":     "https://fsadata.github.io/pig-conditions/data",
}


def _build_candidates(dataset, year, quarter):
    """Return list of candidate URLs for a given dataset/year/quarter (ints)."""
    base_url = BASE_URLS[dataset]
    key = (dataset, year, quarter)
    if key in SPECIAL_CASES:
        return [f"{base_url}/{SPECIAL_CASES[key]}"]

    q_init, q_fin = QUARTER_MONTHS[quarter]
    q_fin_variants = [q_fin, q_fin.replace("sep", "sept")]
    return (
        [f"{base_url}/{dataset}-conditions-{q_init}{year}-{qf}{year}.csv"   for qf in q_fin_variants] +
        [f"{base_url}/{dataset}-conditions-{q_init}-{year}-{qf}-{year}.csv" for qf in q_fin_variants]
    )


def _stream_download(url, output_path):
    """Stream-download url to output_path. Returns True if successful."""
    try:
        with requests.get(url, stream=True, timeout=30) as r:
            if r.status_code == 200:
                size = 0
                with open(output_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                        size += len(chunk)
                print(f"  Downloaded: {os.path.basename(output_path)} ({size // 1024}K)", flush=True)
                return True
    except Exception as e:
        print(f"  Error fetching {url}: {e}", flush=True)
    return False


def download_one(dataset, year, quarter, output_dir="."):
    """Download a single file and save as {dataset}_{year}_Q{quarter}.csv."""
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{dataset}_{year}_Q{quarter}.csv")
    candidates = _build_candidates(dataset, int(year), int(quarter))

    for url in candidates:
        if _stream_download(url, output_path):
            return

    raise RuntimeError(
        f"No file found for {dataset} 20{year} Q{quarter} — tried: {candidates}"
    )


def download_all(output_dir="."):
    """Download all species/years/quarters, saving as {dataset}_{year}_Q{quarter}.csv."""
    os.makedirs(output_dir, exist_ok=True)

    for dataset in BASE_URLS:
        print(f"\n=== {dataset.upper()} ===", flush=True)
        for year in range(17, 21):
            for quarter in range(1, 5):
                candidates = _build_candidates(dataset, year, quarter)
                output_path = os.path.join(output_dir, f"{dataset}_{year}_Q{quarter}.csv")
                downloaded = False
                for url in candidates:
                    if _stream_download(url, output_path):
                        downloaded = True
                        break
                if not downloaded:
                    q_init, q_fin = QUARTER_MONTHS[quarter]
                    print(f"  Not found: {dataset} 20{year} Q{quarter} ({q_init}-{q_fin})", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download FSA livestock condition data.")
    parser.add_argument(
        "--output-dir",
        default=os.environ.get("OUTPUT_DIR", "./data"),
        help="Directory to save downloaded files (default: $OUTPUT_DIR or ./data)",
    )
    parser.add_argument("--species", choices=["pig", "poultry"],
                        help="Single species to download (requires --year and --quarter)")
    parser.add_argument("--year",
                        help="2-digit year, e.g. 18 for 2018 (requires --species and --quarter)")
    parser.add_argument("--quarter", choices=["1", "2", "3", "4"],
                        help="Quarter number (requires --species and --year)")
    args = parser.parse_args()

    single_mode = any([args.species, args.year, args.quarter])
    if single_mode:
        if not all([args.species, args.year, args.quarter]):
            parser.error("--species, --year, and --quarter must all be provided together")
        print(f"Saving to: {args.output_dir}")
        download_one(args.species, args.year, args.quarter, args.output_dir)
    else:
        print(f"Saving all files to: {args.output_dir}")
        download_all(args.output_dir)

    print("\nDone.")
