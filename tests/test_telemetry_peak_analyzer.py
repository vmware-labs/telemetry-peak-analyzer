# Copyright 2021 VMware, Inc.
# SPDX-License-Identifier: BSD-2
import configparser
import datetime
import unittest

import ddt
import mock
from telemetry_peak_analyzer import analyzers
from telemetry_peak_analyzer import backends
from telemetry_peak_analyzer import models


TEST_GLOBAL_TABLE_1 = {
    "malicious": {
        "file_type": models.GlobalTable(
            start_ts=datetime.datetime.strptime("2020-06-17", "%Y-%m-%d"),
            end_ts=datetime.datetime.strptime("2020-07-12", "%Y-%m-%d"),
            window_count=25,
            sub_count_avg=1,
            sub_count_max=10,
            samp_count_max=10,
            samp_count_avg=4,
            samp_sub_count_avg=0,
            samp_sub_count_max=0.0,
            threshold_suggested=10,
        )
    }
}

TEST_GLOBAL_STATS_1 = {
    "malicious": {
        "file_type": models.GlobalTableStats(
            samp_sub_count_max=0.0,
            threshold=10,
        )
    }
}

TEST_LOCAL_TABLE_1 = {
    "malicious": {
        "file_type": {
            "file.sha1": {
                "s1": 10,
                "s2": 10,
            },
            "source.user_id": {
                "u1": 5,
                "u2": 5,
            },
            "source.origin": {
                "o1": 5,
                "o2": 5,
            },
        }
    },
}

TEST_LOCAL_STATS_1 = {
    "malicious": {
        "file_type": models.LocalTableStats(
            sub_count=20,
            samp_count=2,
            samp_sub_count_max=10,
            samp_sub_count_mean=10,
            samp_sub_count_std=0.0,
            samp_sub_ratio=0.0,
            cross_stats={
                "source.user_id": {
                    "u1": 5,
                    "u2": 5,
                },
                "source.origin": {
                    "o1": 5,
                    "o2": 5,
                },
            },
        )
    }
}

TEST_PEAKS_1 = {
    "malicious": {
        "file_type": models.TelemetryPeak(
            sub_count=20,
            samp_count=2,
            samp_sub_count_max=10,
            samp_sub_count_mean=10,
            samp_sub_count_std=0.0,
            samp_sub_ratio=0.0,
            global_samp_sub_count_max=0.0,
            global_threshold_suggested=10,
        )
    }
}

TEST_GLOBAL_TABLE_2 = {
    "malicious": {},
}

TEST_GLOBAL_STATS_2 = {}

TEST_LOCAL_TABLE_2 = {}

TEST_LOCAL_STATS_2 = {}

TEST_PEAKS_2 = {}

TEST_GLOBAL_TABLE_3 = {}

TEST_GLOBAL_STATS_3 = {}

TEST_LOCAL_TABLE_3 = {}

TEST_LOCAL_STATS_3 = {}

TEST_PEAKS_3 = {}


@ddt.ddt
class TestFileTypePeakAnalyzerTinaBackend(unittest.TestCase):
    """Class to test the manager."""

    @ddt.data(
        (TEST_GLOBAL_TABLE_1, TEST_GLOBAL_STATS_1),
        (TEST_GLOBAL_TABLE_2, TEST_GLOBAL_STATS_2),
        (TEST_GLOBAL_TABLE_3, TEST_GLOBAL_STATS_3),
    )
    def test_get_global_tables_stats(self, args):
        """Test the 'get_global_tables_stats' method."""
        global_tables, expected_stats = args
        backend_mock = mock.MagicMock(spec=backends.TwoIndexTwoDimensionBackend)
        peak_analyzer = analyzers.FileTypePeakAnalyzer(
            conf=configparser.ConfigParser(),
            backend=backend_mock,
            start_ts=datetime.datetime.utcnow() - datetime.timedelta(days=7),
            end_ts=datetime.datetime.utcnow(),
        )
        stats = peak_analyzer.get_global_tables_stats(global_tables)
        self.assertEqual(stats, expected_stats)

    @ddt.data(
        (TEST_LOCAL_TABLE_1, TEST_LOCAL_STATS_1),
        (TEST_LOCAL_TABLE_2, TEST_LOCAL_STATS_2),
        (TEST_LOCAL_TABLE_3, TEST_LOCAL_STATS_3),
    )
    def test_get_local_tables_stats(self, args):
        """Test the 'get_local_tables_stats' method."""
        local_tables, expected_stats = args
        backend_mock = mock.MagicMock(spec=backends.TwoIndexTwoDimensionBackend)
        peak_analyzer = analyzers.FileTypePeakAnalyzer(
            conf=configparser.ConfigParser(),
            backend=backend_mock,
            start_ts=datetime.datetime.utcnow() - datetime.timedelta(days=7),
            end_ts=datetime.datetime.utcnow(),
        )
        stats = peak_analyzer.get_local_tables_stats(local_tables)
        self.assertEqual(stats, expected_stats)

    @ddt.data(
        (TEST_GLOBAL_TABLE_1, TEST_LOCAL_TABLE_1, TEST_PEAKS_1),
        (TEST_GLOBAL_TABLE_2, TEST_LOCAL_TABLE_2, TEST_PEAKS_2),
        (TEST_GLOBAL_TABLE_3, TEST_LOCAL_TABLE_3, TEST_PEAKS_3),
    )
    def test_get_peaks(self, args):
        """Test the 'get_peaks' method."""
        global_tables, local_tables, expected_peaks = args
        backend_mock = mock.MagicMock(spec=backends.TwoIndexTwoDimensionBackend)
        peak_analyzer = analyzers.FileTypePeakAnalyzer(
            conf=configparser.ConfigParser(),
            backend=backend_mock,
            start_ts=datetime.datetime.utcnow() - datetime.timedelta(days=7),
            end_ts=datetime.datetime.utcnow(),
        )
        peaks = peak_analyzer.get_peaks(global_tables, local_tables)
        self.assertEqual(peaks, expected_peaks)


if __name__ == "__main__":
    unittest.main()
