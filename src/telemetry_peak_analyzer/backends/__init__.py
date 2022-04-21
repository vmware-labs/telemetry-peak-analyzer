# Copyright 2021 VMware, Inc.
# SPDX-License-Identifier: BSD-2
import abc
import collections
import configparser
import datetime
import glob
import logging
import os
import statistics
from typing import Any
from typing import Dict
from typing import List
from typing import TypeVar

import ijson
import telemetry_peak_analyzer

BackendType = TypeVar("BackendType", bound="AbstractBackend")


class AbstractBackend(abc.ABC):
    """Abstract backend."""

    def __init__(self, conf: configparser.ConfigParser, section_name: str) -> None:
        """
        Constructor.

        :param configparser.ConfigParser conf: the conf object
        :param str section_name: the name of the section
        """
        self._conf = conf
        self._section_name = section_name
        self._logger = logging.getLogger(__name__)
        self._logger.info("Loading backend '%s'", self.__class__.__name__)

    @abc.abstractmethod
    def stats(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        index: List[str],
        dimensions: List[str],
        dimensions_values: Dict[str, List[str]],
    ) -> Dict[str, Any]:
        """
        Create statistics.

        :param datetime.datetime start_date: the start of the time interval
        :param datetime.datetime end_date: the end of the time interval
        :param list[str] index: the index
        :param list[str] dimensions: the dimensions
        :param dict[str, list[str]] dimensions_values: the values of dimensions (if available)
        :rtype: dict[str, any]
        :return: statistics for each dimension combination
        """

    @abc.abstractmethod
    def group_by(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        index: List[str],
        dimensions: List[str],
    ) -> List[Dict[str, str]]:
        """
        Group by.

        :param datetime.datetime start_date: the start of the time interval
        :param datetime.datetime end_date: the end of the time interval
        :param list[str] index: the index
        :param list[str] dimensions: list of dimensions to group by
        :rtype: list[dict[str, str]]
        :return: buckets for each dimension combination
        """


class TwoIndexTwoDimensionBackend(AbstractBackend, abc.ABC):
    """Backend accepting an index with two fields and two dimensions."""

    @abc.abstractmethod
    def stats(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        index: List[str],
        dimensions: List[str],
        dimensions_values: Dict[str, List[str]],
    ) -> Dict[str, Dict[str, Dict[str, float]]]:
        """
        Create statistics.

        :param datetime.datetime start_date: the start of the time interval
        :param datetime.datetime end_date: the end of the time interval
        :param list[str] index: the index
        :param list[str] dimensions: the dimensions
        :param dict[str, list[str]] dimensions_values: the values of dimensions (if available)
        :rtype: dict[str, dict[str, dict[str, float]]]
        :return: a dictionary like:
                {
                    "dimension_0_value": {
                        "dimension_1_value": {
                            "sub_count_avg": average of submissions
                            "sub_count_max": maximum number of submissions
                            "samp_count_avg": average of samples
                            "samp_count_max": maximum number of samples
                            "samp_sub_count_max": maximum ratio between samples and submissions
                        }
                        ...
                    }
                    ...
                }
        """

    @abc.abstractmethod
    def group_by(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        index: List[str],
        dimensions: List[str],
    ) -> List[Dict[str, str]]:
        """
        Group by.

        :param datetime.datetime start_date: the start of the time interval
        :param datetime.datetime end_date: the end of the time interval
        :param list[str] index: the index
        :param list[str] dimensions: the dimensions
        :rtype: list[dict[str, str]]
        :return: a list of dictionaries like:
            [
                {
                  "dimension_0": "benign",
                  "dimension_1": "ExcelMsDocFile",
                  "index_1": "0015cc85a17d707e00b9881a149c232d181ad451",
                  "additional_dimension_0": "3549",
                  "additional_dimension_1": "API",
                  "count": 61
                }
                ...
            ]
        """


class JsonBackend(TwoIndexTwoDimensionBackend):
    """Backend using JSON files."""

    def __init__(self, conf: configparser.ConfigParser, file_path_wildcard: str) -> None:
        """Constructor."""
        super(JsonBackend, self).__init__(conf, section_name="not_used")
        self._file_paths = []
        for name in glob.glob(file_path_wildcard):
            self._file_paths.append(os.path.abspath(name))
        self._logger.info("Loaded files:")
        for file_path in self._file_paths:
            self._logger.info("\t%s", file_path)

    def stats(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        index: List[str],
        dimensions: List[str],
        dimensions_values: Dict[str, List[str]],
    ) -> Dict[str, Dict[str, Dict[str, float]]]:
        """Implement interface."""
        # we do several passes so to keep memory usage to a minimum
        # pass 1, let us get all the dates
        dates = set([])
        for file_path in self._file_paths:
            with open(file_path, "r") as f:
                for json_doc in ijson.items(f, "item"):
                    index_0 = telemetry_peak_analyzer.ms_to_datetime(json_doc[index[0]])
                    if start_date <= index_0 < end_date:
                        dates.add(index_0.date())

        # pass 2, for each date get the stats
        buckets = collections.defaultdict(lambda: collections.defaultdict(dict))
        all_dims_0 = set([])
        all_dims_1 = set([])
        for day_date in sorted(dates):
            sub_count = collections.defaultdict(lambda: collections.defaultdict(int))
            samp_set = collections.defaultdict(lambda: collections.defaultdict(set))
            dims_0 = set([])
            dims_1 = set([])
            for file_path in self._file_paths:
                with open(file_path, "r") as f:
                    for json_doc in ijson.items(f, "item"):
                        index_0 = telemetry_peak_analyzer.ms_to_datetime(
                            json_doc[index[0]]
                        ).date()
                        index_1 = json_doc[index[1]]
                        dimension_0 = json_doc[dimensions[0]]
                        dimension_1 = json_doc[dimensions[1]]
                        if index_0 == day_date:
                            sub_count[dimension_0][dimension_1] += 1
                            samp_set[dimension_0][dimension_1].add(index_1)
                            dims_0.add(dimension_0)
                            dims_1.add(dimension_1)
            for dim_0 in dims_0:
                for dim_1 in dims_1:
                    try:
                        samp_sub_count = sub_count[dim_0][dim_1] / len(samp_set[dim_0][dim_1])
                    except ZeroDivisionError:
                        samp_sub_count = 0
                    buckets[day_date][dim_0][dim_1] = {
                        "sub_count": sub_count[dim_0][dim_1],
                        "samp_count": len(samp_set[dim_0][dim_1]),
                        "samp_sub_count": samp_sub_count,
                    }
            all_dims_0.update(dims_0)
            all_dims_1.update(dims_1)
        ret = collections.defaultdict(dict)
        for dim_0 in all_dims_0:
            for dim_1 in all_dims_1:
                d_slice = [buckets[x].get(dim_0, {}).get(dim_1, {}) for x in dates]
                ret[dim_0][dim_1] = {
                    "sub_count_avg": statistics.mean([x.get("sub_count", 0) for x in d_slice]),
                    "sub_count_max": max([x.get("sub_count", 0) for x in d_slice]),
                    "samp_count_avg": statistics.mean([x.get("samp_count", 0) for x in d_slice]),
                    "samp_count_max": max([x.get("samp_count", 0) for x in d_slice]),
                    "samp_sub_count_max": max([x.get("samp_sub_count", 0) for x in d_slice]),
                }
        return ret

    def group_by(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        index: List[str],
        dimensions: List[str],
    ) -> List[Dict[str, str]]:
        """Implement interface."""
        counters = collections.Counter()
        dimensions = dimensions + [index[1]]
        for file_path in self._file_paths:
            with open(file_path, "r") as f:
                for json_doc in ijson.items(f, "item"):
                    index_0 = telemetry_peak_analyzer.ms_to_datetime(json_doc[index[0]])
                    if start_date <= index_0 < end_date:
                        counters[tuple([json_doc[dimension] for dimension in dimensions])] += 1
        ret = []
        for key, count in counters.items():
            value = {attr: key[idx] for idx, attr in enumerate(dimensions)}
            value["count"] = count
            ret.append(value)
        return ret
