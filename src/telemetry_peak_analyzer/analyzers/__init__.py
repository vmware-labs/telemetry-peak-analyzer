# Copyright 2021 VMware, Inc.
# SPDX-License-Identifier: BSD-2
import abc
import collections
import configparser
import datetime
import json
import logging
import statistics
from abc import ABC
from typing import Dict
from typing import List
from typing import Optional
from typing import Set

import telemetry_peak_analyzer
from telemetry_peak_analyzer import backends
from telemetry_peak_analyzer import models


class AbstractAnalyzer(abc.ABC):
    """Abstract analyzer."""

    DEFAULT_METRIC_TABLE_AGE = datetime.timedelta(days=10)

    @staticmethod
    def _get_window_count(start_ts: int, end_ts: int) -> int:
        """
        Get the window count.

        :param int start_ts: milliseconds
        :param int end_ts: milliseconds
        :rtype: int
        :return: the window count
        """
        delta_hours = (end_ts - start_ts) // 1000 // 60 // 60
        window_count = delta_hours // 24 if delta_hours % 24 == 0 else delta_hours
        # if start_ts and end_ts from the buckets are the same, then count it as one window
        if window_count == 0:
            window_count = 1
        return window_count

    def __init__(
        self,
        conf: configparser.ConfigParser,
        backend: backends.BackendType,
        index: List[str],
        dimensions: List[str],
        start_ts: datetime.datetime,
        end_ts: datetime.datetime,
    ) -> None:
        """
        Constructor.

        :param configparser.ConfigParser conf: the conf object
        :param BackendType backend: the backend
        :param list[str] index: the index
        :param list[str] dimensions: the dimensions
        :param datetime.datetime start_ts: the beginning of the time interval
        :param datetime.datetime end_ts: the end of the time interval
        """
        self._conf = conf
        self._backend = backend
        self._index = index
        self._dimensions = dimensions
        self._start_ts = start_ts
        self._end_ts = end_ts
        self._logger = logging.getLogger(__name__)
        self._logger.info(
            "Loading analyzer '%s' with backend '%s'",
            self.__class__.__name__,
            self._backend.__class__.__name__,
        )

    @abc.abstractmethod
    def get_peaks(
        self,
        global_tables: Dict,
        local_tables: Dict,
        threshold: Optional[int] = None,
    ) -> Dict:
        """
        Get the peaks.

        :param dict global_tables: the global tables
        :param dict local_tables: the local tables
        :param int|None threshold: optional threshold
        :rtype: dict
        :return: the telemetry peaks
        """

    @abc.abstractmethod
    def refresh_global_tables(self, global_tables: Dict, local_tables: Dict) -> Dict:
        """
        Refresh the global tables.

        :param dict global_tables: the global tables
        :param dict local_tables: the local tables
        :rtype: dict
        :return: the global tables
        """

    @abc.abstractmethod
    def get_global_tables_stats(
        self,
        global_tables: Dict,
        threshold: Optional[int] = None,
    ) -> Dict:
        """
        Get statistics from the global table.

        :param dict global_tables: the global tables
        :param int|None threshold: optional threshold
        :rtype: dict
        :return: the global tables stats
        """

    @abc.abstractmethod
    def get_global_tables(self) -> Dict:
        """
        Get the global tables from our backend.

        :rtype: dict
        :return: global tables
        """

    @abc.abstractmethod
    def get_global_tables_from_file(self, file_path: str) -> Dict:
        """
        Load global tables from a file.

        :param str file_path: the file path where the load from
        :rtype: dict
        :return: the loaded object
        """

    @abc.abstractmethod
    def get_local_tables_stats(self, local_tables: Dict) -> Dict:
        """
        Get statistics from the local table.

        :param dict local_tables: local tables
        :rtype: dict
        :return: the local tables stats
        """

    @abc.abstractmethod
    def get_local_tables(self) -> Dict:
        """
        Get the local tables from our backend.

        :rtype: dict
        :return: the local tables
        """


class TwoIndexTwoDimensionAnalyzer(AbstractAnalyzer, ABC):
    """Analyzer using index and dimension with cardinality set to two."""

    LOCAL_MIN_COUNT = 50

    PEAK_GLOBAL_COUNT_WEIGHT = 0.8

    PEAK_MIN_SAMP_SUB_RATIO = 0.5

    CROSS_DIMENSIONS = []

    DIMENSIONS_METADATA = {}

    def _get_dimension_threshold(self, dimension: str, value: str) -> int:
        """Get the threshold for a given dimension and value, if available, 0 otherwise."""
        try:
            return self.DIMENSIONS_METADATA[dimension]["threshold"][value]
        except KeyError:
            return 0

    def _get_dimension_values(self, dimension: str) -> List[str]:
        """Get all values that a dimension can exhibit, if available."""
        try:
            return self.DIMENSIONS_METADATA[dimension]["values"]
        except KeyError:
            return []

    def _is_peak(
        self,
        local_table_stats: models.LocalTableStats,
        global_table_stats: models.GlobalTableStats,
    ) -> bool:
        """
        Return whether the peak is valid.

        :param LocalTableStats local_table_stats: stats about the local table
        :param GlobalTableStats global_table_stats: stats about the global table
        :rtype: bool
        :return: whether the peak is valid
        """
        if local_table_stats.sub_count < global_table_stats.threshold:
            return False
        temp = self.PEAK_GLOBAL_COUNT_WEIGHT * global_table_stats.samp_sub_count_max
        is_susp_overall_sub_samp_r = local_table_stats.samp_sub_count_mean > temp
        is_dominant_samp_sub = local_table_stats.samp_sub_ratio > self.PEAK_MIN_SAMP_SUB_RATIO
        temp = local_table_stats.samp_sub_count_mean + local_table_stats.samp_sub_count_std
        is_susp_samp_sub_var = local_table_stats.samp_sub_count_max > temp
        return is_susp_overall_sub_samp_r or is_dominant_samp_sub or is_susp_samp_sub_var

    def _update_global_table(
        self,
        global_table: models.GlobalTable,
        local_table: Dict[str, Dict[str, int]],
    ) -> models.GlobalTable:
        """
        Update the global table

        :param GlobalTable global_table: the global table
        :param dict[str, dict[str, int]] local_table: the local table
        :rtype: GlobalTable
        :return: a refreshed global table
        """
        window_count = global_table.window_count + 1
        sub_count = sum(local_table[self._index[1]].values())
        samp_count = len(local_table[self._index[1]])
        try:
            samp_sub_count = int(round(sub_count / samp_count))
        except ZeroDivisionError:
            samp_sub_count = 0
        sub_count_avg = int(
            round(
                (global_table.sub_count_avg * global_table.window_count + sub_count)
                / window_count
            )
        )
        samp_count_avg = int(
            round(
                (global_table.samp_count_avg * global_table.window_count + samp_count)
                / window_count
            )
        )
        samp_sub_count_avg = int(
            round(
                (global_table.samp_sub_count_avg * global_table.window_count + samp_sub_count)
                / window_count
            )
        )
        return models.GlobalTable(
            start_ts=global_table.start_ts,
            end_ts=self._end_ts,
            window_count=window_count,
            sub_count_avg=sub_count_avg,
            sub_count_max=max(global_table.sub_count_max, sub_count),
            samp_count_avg=samp_count_avg,
            samp_count_max=max(global_table.samp_count_max, samp_count),
            samp_sub_count_avg=samp_sub_count_avg,
            samp_sub_count_max=max(global_table.samp_sub_count_max, samp_sub_count),
            threshold_suggested=max(global_table.threshold_suggested, sub_count_avg),
        )

    def _infer_global_table(
        self,
        local_table: Dict[str, Dict[str, int]],
        threshold: int,
    ) -> models.GlobalTable:
        """
        Infer the global table from the local table.

        :param dict[str, dict[str, int]] local_table: the local table
        :param int threshold: the threshold to suggest
        :rtype: GlobalTable
        :return: the inferred global table
        """
        sub_count = sum(local_table[self._index[1]].values())
        samp_count = len(local_table[self._index[1]])
        try:
            samp_sub_count = int(round(sub_count / samp_count))
        except ZeroDivisionError:
            samp_sub_count = 0
        return models.GlobalTable(
            start_ts=self._start_ts,
            end_ts=self._end_ts,
            window_count=1,
            sub_count_avg=sub_count,
            sub_count_max=sub_count,
            samp_count_avg=samp_count,
            samp_count_max=samp_count,
            samp_sub_count_avg=samp_sub_count,
            samp_sub_count_max=samp_sub_count,
            threshold_suggested=max(sub_count, threshold),
        )

    def get_peaks(
        self,
        global_tables: Dict[str, Dict[str, models.GlobalTable]],
        local_tables: Dict[str, Dict[str, Dict[str, Dict[str, int]]]],
        threshold: Optional[int] = None,
    ) -> Dict[str, Dict[str, models.TelemetryPeak]]:
        """
        Get the peaks.

        :param dict[str, dict[str, GlobalTable]] global_tables: the global tables
        :param dict[str, dict[str, dict[str, dict[str, int]]]] local_tables: the local tables
        :param int|None threshold: optional threshold
        :rtype: dict[str, dict[str, TelemetryPeak]]
        :return: the telemetry peaks
        """
        local_tables_stats = self.get_local_tables_stats(local_tables)
        global_tables_stats = self.get_global_tables_stats(global_tables, threshold=threshold)
        peaks = collections.defaultdict(dict)
        for dimension_0 in local_tables_stats:
            for dimension_1 in local_tables_stats[dimension_0]:
                try:
                    local_table_stats = local_tables_stats[dimension_0][dimension_1]
                    global_table_stats = global_tables_stats[dimension_0][dimension_1]
                except KeyError:
                    continue
                if self._is_peak(local_table_stats, global_table_stats):
                    peaks[dimension_0][dimension_1] = models.TelemetryPeak(
                        sub_count=local_table_stats.sub_count,
                        samp_count=local_table_stats.samp_count,
                        samp_sub_count_max=local_table_stats.samp_sub_count_max,
                        samp_sub_count_mean=local_table_stats.samp_sub_count_mean,
                        samp_sub_count_std=local_table_stats.samp_sub_count_std,
                        samp_sub_ratio=local_table_stats.samp_sub_ratio,
                        global_samp_sub_count_max=global_table_stats.samp_sub_count_max,
                        global_threshold_suggested=global_table_stats.threshold,
                    )
        return peaks

    def refresh_global_tables(
        self,
        global_tables: Dict[str, Dict[str, models.GlobalTable]],
        local_tables: Dict[str, Dict[str, Dict[str, Dict[str, int]]]],
    ) -> Dict[str, Dict[str, models.GlobalTable]]:
        """
        Refresh the global tables.

        :param dict[str, dict[str, GlobalTable]] global_tables: the global tables
        :param dict[str, dict[str, dict[str, dict[str, int]]]] local_tables: the local tables
        :rtype: dict[str, dict[str, GlobalTable]]
        :return: the global tables
        """

        def get_dimensions_0() -> Set[str]:
            """Get the first dimension from all the global and local tables."""
            return set(global_tables.keys()).union(local_tables.keys())

        def get_dimensions_1(dim_0: str) -> Set[str]:
            """Get the second dimension from all the global and local tables."""
            return set(global_tables.get(dim_0, {}).keys()).union(
                local_tables.get(dim_0, {}).keys()
            )

        refreshed_global_tables = collections.defaultdict(dict)
        for dimension_0 in get_dimensions_0():
            for dimension_1 in get_dimensions_1(dimension_0):
                global_table = global_tables.get(dimension_0, {}).get(dimension_1, None)
                local_table = local_tables.get(dimension_0, {}).get(dimension_1, None)
                # If we have both tables for all dimensions, check their age
                if global_table and local_table:
                    # if the global table is more recent than the current time interval
                    if self._start_ts < global_table.end_ts:
                        new_table = global_table
                    # otherwise update the global table
                    else:
                        new_table = self._update_global_table(global_table, local_table)
                # if we only have the global table, then use the global table
                elif global_table and not local_table:
                    new_table = global_table
                # and if we only have the local table, infer a global table
                elif not global_table and local_table:
                    threshold = self._get_dimension_threshold(self._dimensions[0], dimension_0)
                    new_table = self._infer_global_table(local_table, threshold)
                # otherwise just skip
                else:
                    continue
                refreshed_global_tables[dimension_0][dimension_1] = new_table
        return refreshed_global_tables

    def get_global_tables_stats(
        self,
        global_tables: Dict[str, Dict[str, models.GlobalTable]],
        threshold: Optional[int] = None,
    ) -> Dict[str, Dict[str, models.GlobalTableStats]]:
        """
        Get statistics from the global table.

        :param dict[str, dict[str, GlobalTable]] global_tables: the global tables
        :param int|None threshold: optional threshold
        :rtype: dict[str, dict[str, GlobalTableStats]]
        :return: the global tables stats
        """
        global_stats = collections.defaultdict(dict)
        for dimension_0 in global_tables:
            for dimension_1 in global_tables[dimension_0]:
                global_table = global_tables[dimension_0][dimension_1]
                global_stats[dimension_0][dimension_1] = models.GlobalTableStats(
                    threshold=threshold or global_table.threshold_suggested,
                    samp_sub_count_max=global_table.samp_sub_count_max,
                )
        return global_stats

    def get_global_tables_from_file(
        self,
        file_path: str,
    ) -> Dict[str, Dict[str, models.GlobalTable]]:
        """
        Load global tables from a file.

        :param str file_path: the file path where the load from
        :rtype: dict[str, dict[str, GlobalTable]]
        :return: the global tables
        """
        with open(file_path, "r") as f:
            json_data = json.load(f)
        for dimension_0 in json_data:
            for dimension_1 in json_data[dimension_0]:
                table = json_data[dimension_0][dimension_1]
                json_data[dimension_0][dimension_1] = models.GlobalTable(
                    start_ts=datetime.datetime.strptime(table[0][:19], "%Y-%m-%d %H:%M:%S"),
                    end_ts=datetime.datetime.strptime(table[1][:19], "%Y-%m-%d %H:%M:%S"),
                    window_count=table[2],
                    sub_count_avg=table[3],
                    sub_count_max=table[4],
                    samp_count_avg=table[5],
                    samp_count_max=table[6],
                    samp_sub_count_avg=table[7],
                    samp_sub_count_max=table[8],
                    threshold_suggested=table[9],
                )
        return json_data

    def get_global_tables(self) -> Dict[str, Dict[str, models.GlobalTable]]:
        """
        Get the global tables.

        :rtype: dict[str, dict[str, GlobalTable]]
        :return: the global tables
        """
        end_date = datetime.datetime.utcnow()
        start_date = end_date - self.DEFAULT_METRIC_TABLE_AGE
        ret = self._backend.stats(
            start_date=start_date,
            end_date=end_date,
            index=self._index,
            dimensions=self._dimensions,
            dimensions_values={x: self._get_dimension_values(x) for x in self._dimensions},
        )
        global_tables = collections.defaultdict(dict)
        for dimension_0 in ret:
            for dimension_1 in ret[dimension_0]:
                start_ts = telemetry_peak_analyzer.datetime_to_ms(start_date)
                end_ts = telemetry_peak_analyzer.datetime_to_ms(end_date)
                threshold = self._get_dimension_threshold(self._dimensions[0], dimension_0)
                sub_count_avg = ret[dimension_0][dimension_1]["sub_count_avg"]
                sub_count_max = ret[dimension_0][dimension_1]["sub_count_max"]
                samp_count_avg = ret[dimension_0][dimension_1]["samp_count_avg"]
                samp_count_max = ret[dimension_0][dimension_1]["samp_count_max"]
                samp_sub_count_max = ret[dimension_0][dimension_1]["samp_sub_count_max"]
                try:
                    samp_sub_count_avg = int(round(sub_count_avg / samp_count_avg))
                except ZeroDivisionError:
                    samp_sub_count_avg = 0
                global_tables[dimension_0][dimension_1] = models.GlobalTable(
                    start_ts=start_date,
                    end_ts=end_date,
                    window_count=self._get_window_count(start_ts, end_ts),
                    sub_count_avg=int(round(sub_count_avg)),
                    sub_count_max=int(sub_count_max),
                    samp_count_avg=int(round(samp_count_avg)),
                    samp_count_max=int(samp_count_max),
                    samp_sub_count_avg=samp_sub_count_avg,
                    samp_sub_count_max=samp_sub_count_max,
                    threshold_suggested=max(int(round(sub_count_avg)), threshold),
                )
        return global_tables

    def get_local_tables_stats(
        self, local_tables: Dict[str, Dict[str, Dict[str, Dict[str, int]]]]
    ) -> Dict[str, Dict[str, models.LocalTableStats]]:
        """
        Get statistics from the local table.

        :param dict[str, dict[str, dict[str, dict[str, int]]]] local_tables: local tables
        :rtype: dict[str, dict[str, LocalTableStats]]
        :return: some statistics for each dimension combination
        """
        local_stats = collections.defaultdict(dict)
        for dimension_0 in local_tables:
            for dimension_1 in local_tables[dimension_0]:
                local_table = local_tables[dimension_0][dimension_1]
                sub_count = sum(local_table[self._index[1]].values())
                samp_count = len(local_table[self._index[1]])
                samp_sub_count = local_table[self._index[1]].values()
                # fails if len < 1, one fails, both fail
                try:
                    samp_sub_count_mean = statistics.mean(samp_sub_count)
                    samp_sub_count_max = max(samp_sub_count)
                except (statistics.StatisticsError, ValueError):
                    samp_sub_count_mean = 0.0
                    samp_sub_count_max = 0
                # fails if len < 2
                try:
                    samp_sub_count_std = statistics.stdev(samp_sub_count)
                except statistics.StatisticsError:
                    samp_sub_count_std = 0.0
                if sub_count > self.LOCAL_MIN_COUNT:
                    samp_sub_ratio = round(samp_sub_count_max / sub_count, 2)
                else:
                    samp_sub_ratio = 0.0
                local_stats[dimension_0][dimension_1] = models.LocalTableStats(
                    sub_count=sub_count,
                    samp_count=samp_count,
                    samp_sub_count_max=samp_sub_count_max,
                    samp_sub_count_mean=samp_sub_count_mean,
                    samp_sub_count_std=samp_sub_count_std,
                    samp_sub_ratio=samp_sub_ratio,
                    cross_stats={x: local_table[x] for x in self.CROSS_DIMENSIONS},
                )
        return local_stats

    def get_local_tables(self) -> Dict[str, Dict[str, Dict[str, Dict[str, int]]]]:
        """
        Get the local tables from our backend.

        :rtype: dict[str, dict[str, dict[str, dict[str, int]]]]
        :return: the local tables
        """
        terms = self._dimensions + self.CROSS_DIMENSIONS + [self._index[1]]
        json_data = self._backend.group_by(
            start_date=self._start_ts,
            end_date=self._end_ts,
            index=self._index,
            dimensions=self._dimensions + self.CROSS_DIMENSIONS,
        )
        local_tables = collections.defaultdict(dict)
        for item in json_data:
            dimension_0 = item[self._dimensions[0]]
            dimension_1 = item[self._dimensions[1]]
            if dimension_1 not in local_tables[dimension_0]:
                local_tables[dimension_0][dimension_1] = {
                    term: collections.defaultdict(int) for term in terms
                }
            for term in terms:
                local_tables[dimension_0][dimension_1][term][item[term]] += item["count"]
        return local_tables


class FileTypePeakAnalyzer(TwoIndexTwoDimensionAnalyzer):
    """Analyzer using index and dimension to track file types."""

    CROSS_DIMENSIONS = [
        "source.user_id",
        "source.origin",
    ]

    DIMENSIONS_METADATA = {
        "task.severity": {
            "values": ["malicious", "benign"],
            "threshold": {
                "malicious": 90,
                "benign": 500,
            },
        }
    }

    _INDEX = [
        "utc_timestamp",
        "file.sha1",
    ]

    _DIMENSIONS = [
        "task.severity",
        "file.llfile_type",
    ]

    def __init__(
        self,
        conf: configparser.ConfigParser,
        backend: backends.BackendType,
        start_ts: datetime.datetime,
        end_ts: datetime.datetime,
    ) -> None:
        """
        Constructor.

        :param configparser.ConfigParser conf: the conf object
        :param backendType backend: the backend
        :param datetime.datetime start_ts: the beginning of the time interval
        :param datetime.datetime end_ts: the end of the time interval
        """
        if not isinstance(backend, backends.TwoIndexTwoDimensionBackend):
            raise ValueError("Backend is not compatible with the chosen analyzer")
        super(FileTypePeakAnalyzer, self).__init__(
            conf=conf,
            index=self._INDEX,
            dimensions=self._DIMENSIONS,
            backend=backend,
            start_ts=start_ts,
            end_ts=end_ts,
        )
