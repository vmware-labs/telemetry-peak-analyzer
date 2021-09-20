# Copyright 2021 VMware, Inc.
# SPDX-License-Identifier: BSD-2
import collections


TelemetryPeak = collections.namedtuple(
    "TelemetryPeak",
    [
        "sub_count",
        "samp_count",
        "samp_sub_count_max",
        "samp_sub_count_mean",
        "samp_sub_count_std",
        "samp_sub_ratio",
        "global_samp_sub_count_max",
        "global_threshold_suggested",
    ],
)


GlobalTable = collections.namedtuple(
    "GlobalTable",
    [
        "start_ts",
        "end_ts",
        "window_count",
        "sub_count_avg",
        "sub_count_max",
        "samp_count_avg",
        "samp_count_max",
        "samp_sub_count_avg",
        "samp_sub_count_max",
        "threshold_suggested",
    ],
)


LocalTableStats = collections.namedtuple(
    "LocalTableStats",
    [
        "sub_count",
        "samp_count",
        "samp_sub_count_max",
        "samp_sub_count_mean",
        "samp_sub_count_std",
        "samp_sub_ratio",
        "cross_stats",
    ],
)


GlobalTableStats = collections.namedtuple(
    "GlobalTableStats",
    [
        "samp_sub_count_max",
        "threshold",
    ],
)
