#!/usr/bin/env python
# Copyright 2021 VMware, Inc.
# SPDX-License-Identifier: BSD-2
import argparse
import hashlib
import json
import os
import sys


def main() -> int:
    """Anonymize telemetry JSON files."""
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
    args = parser.parse_args()

    with open(args.input_file, "r") as f:
        telemetry_data = json.load(f)

    for item in telemetry_data:
        item["customer.channel"] = None
        item["customer.installation_type"] = None
        item["customer.type"] = None
        item["file.md5"] = hashlib.md5(item["file.md5"].encode("utf-8")).hexdigest()
        item["file.name"] = None
        item["file.sha1"] = hashlib.sha1(item["file.sha1"].encode("utf-8")).hexdigest()
        if item["file.sha256"]:
            item["file.sha256"] = hashlib.sha256(item["file.sha256"].encode("utf-8")).hexdigest()
        item["file.size"] = 0
        item["source.access_key_id"] = 0
        item["source.data_center"] = None
        item["source.geo.country_iso_code"] = None
        item["source.geo.location"] = "0.00,0.00"
        item["source.submitter_ip"] = "0.0.0.0"
        item["source.user_id"] = 0
        item["submission_id"] = 0
        item["task.portal_url"] = None
        item["task.uuid"] = "a" * 32

    file_path = f"{os.path.splitext(args.input_file)[0]}.anonymized.json"
    with open(file_path, "w") as f:
        json.dump(telemetry_data, f, indent=2, sort_keys=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
