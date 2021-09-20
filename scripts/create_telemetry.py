#!/usr/bin/env python
# Copyright 2021 VMware, Inc.
# SPDX-License-Identifier: BSD-2
import argparse
import csv
import datetime
import json
import os
import sys

SEVERITY_BENIGN = "benign"
SEVERITY_MALICIOUS = "malicious"
SEVERITY_SUSPICIOUS = "suspicious"
SEVERITY_ALL = [
    SEVERITY_BENIGN,
    SEVERITY_MALICIOUS,
    SEVERITY_SUSPICIOUS,
]


def datetime_str_to_ms(date_str: str, fmt: str = "%Y-%m-%d %H:%M:%S") -> int:
    """
    Convert a given timestamp to milliseconds since the epoch.

    :param str date_str: the datetime string
    :param str fmt: the format
    :rtype: int
    """
    date_obj = datetime.datetime.strptime(date_str, fmt)
    return int((date_obj - datetime.datetime.utcfromtimestamp(0)).total_seconds()) * 1000


def main() -> int:
    """Convert (internal, deprecated) CSV telemetry files into JSON telemetry data."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i",
        "--input-file",
        dest="input_file",
        default=None,
        required=True,
        type=str,
        help="The input file",
    )
    parser.add_argument(
        "-s",
        "--severity-filter",
        dest="severity_filter",
        choices=SEVERITY_ALL,
        default=None,
        help=f"Optional filter by severity ({','.join(SEVERITY_ALL)})",
    )
    args = parser.parse_args()

    telemetry_data = []
    with open(args.input_file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if args.severity_filter and row["severity"] != args.severity_filter:
                continue
            telemetry_data.append(
                {
                    "analysis.label": row["vt_label"],
                    "customer.channel": row["channel"],
                    "customer.installation_type": row["installation_type"],
                    "customer.region": row["region"],
                    "customer.sector": row["sector"],
                    "customer.type": row["key_type"],
                    "file.llfile_type": row["file_type"],
                    "file.magic": None,
                    "file.md5": row["md5"],
                    "file.mime_type": row["mime_type"],
                    "file.name": None,
                    "file.sha1": row["sha1"],
                    "file.sha256": None,
                    "file.size": row["file_size"],
                    "source.access_key_id": row["access_key_id"],
                    "source.data_center": row["data_center"],
                    "source.geo.country_iso_code": None,
                    "source.geo.location": "0.00,0.00",
                    "source.origin": row["origin"],
                    "source.submitter_ip": row["submitter_ip"],
                    "source.user_id": row["user_id"],
                    "submission_id": row["submission_id"],
                    "task.portal_url": None,
                    "task.score": row["score"],
                    "task.severity": row["severity"],
                    "task.uuid": row["task_uuid"],
                    "utc_timestamp": datetime_str_to_ms(row["ts"]),
                }
            )

    file_path = f"{os.path.splitext(args.input_file)[0]}.json"
    with open(file_path, "w") as f:
        json.dump(telemetry_data, f, indent=2, sort_keys=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
