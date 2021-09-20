# Copyright 2021 VMware, Inc.
# SPDX-License-Identifier: BSD-2
import collections
import configparser
import datetime
import logging
from typing import Any
from typing import Dict
from typing import List

from telemetry_peak_analyzer import backends

try:
    from tina_client.storage import readers
except ImportError:
    raise ImportError("The Tina backend requires tina-client.") from None


logging.getLogger("elasticsearch").setLevel(logging.WARNING)


class TinaBackend(backends.TwoIndexTwoDimensionBackend):
    """
    Backend using Tina (an internal Elasticsearch cluster).

    Note: in this class we hide all the specialization required to port the existing logic; note
        that this is unlikely to work with different analyzer and generalizing this bit further
        might not be worth the cost.
    """

    @staticmethod
    def _get_aggregation_query(
        index: List[str],
        dimensions: List[str],
        dimensions_values: Dict[str, List[str]],
    ) -> Dict[str, Dict]:
        """
        Return a fully loaded Elasticsearch aggregation query.

        :param list[str] index: the index
        :param list[str] dimensions: the dimensions
        :param dict[str, list[str]] dimensions_values: the values of dimensions (if available)
        :rtype: dict[str, dict]
        :return: the aggregation to run
        """

        def _get_aggregation() -> Dict[str, Dict]:
            """Return the aggregation for each dimension."""
            return {
                "sub_per_day": {
                    "date_histogram": {
                        "field": index[0],
                        "calendar_interval": "day",
                        "min_doc_count": 0,
                    },
                    "aggs": {
                        "samp_count": {
                            "cardinality": {
                                "field": index[1],
                            }
                        },
                        "samp_sub_count": {
                            "bucket_script": {
                                "buckets_path": {
                                    "sub_count": "_count",
                                    "samp_count": "samp_count",
                                },
                                "script": "params.sub_count/params.samp_count",
                            }
                        },
                    },
                },
                "samp_count_avg": {"avg_bucket": {"buckets_path": "sub_per_day.samp_count"}},
                "samp_count_max": {"max_bucket": {"buckets_path": "sub_per_day.samp_count"}},
                "samp_sub_count_max": {
                    "max_bucket": {"buckets_path": "sub_per_day>samp_sub_count"}
                },
                "sub_count_avg": {"avg_bucket": {"buckets_path": "sub_per_day._count"}},
                "sub_count_max": {"max_bucket": {"buckets_path": "sub_per_day._count"}},
            }

        dimension_0_values = dimensions_values.get(dimensions[0], [])
        return {
            "aggs": {
                "my_buckets": {
                    "composite": {
                        "sources": [{"dimension": {"terms": {"field": dimensions[1]}}}]
                    },
                    "aggs": {
                        dimension_0_values[0]: {
                            "filter": {"term": {dimensions[0]: dimension_0_values[0]}},
                            "aggs": _get_aggregation(),
                        },
                        dimension_0_values[1]: {
                            "filter": {"term": {dimensions[0]: dimension_0_values[1]}},
                            "aggs": _get_aggregation(),
                        },
                    },
                }
            },
            "size": 0,
        }

    @staticmethod
    def _parse_aggregation_output(
        buckets: List[Dict[str, Dict[str, Any]]],
        dimensions: List[str],
        dimensions_values: Dict[str, List[str]],
    ) -> Dict[str, Dict[str, Dict[str, float]]]:
        """
        Parse the aggregation from Elasticsearch into a backend-independent format.

        :param list[dict[str, dict[str, any]]] buckets: the aggregation buckets
        :param list[str] dimensions: the dimensions
        :param dict[str, list[str]] dimensions_values: the values of some dimensions (if available)
        :rtype: dict[str, dict[str, dict[str, float]]]
        :return: the aggregation results indexed by dimension
        """
        aggregation_keys = frozenset(
            [
                "sub_count_avg",
                "sub_count_max",
                "samp_count_avg",
                "samp_count_max",
                "samp_sub_count_max",
            ]
        )
        dimension_0_values = dimensions_values.get(dimensions[0], [])
        ret = collections.defaultdict(dict)
        for bucket in buckets:
            for dimension_0 in dimension_0_values:
                if bucket[dimension_0]["doc_count"] > 0:
                    dimension_1 = bucket["key"]["dimension"]
                    ret[dimension_0][dimension_1] = {
                        k: bucket[dimension_0][k]["value"]
                        for k in bucket[dimension_0]
                        if k in aggregation_keys
                    }
        return ret

    def __init__(self, conf: configparser.ConfigParser, section_name: str) -> None:
        """Constructor."""
        super(TinaBackend, self).__init__(conf, section_name)
        self._tina_reader = readers.BulkFileSubmissionReader(conf, section_name)
        self._logger.info(
            "Loading backend '%s' from section '%s'",
            self.__class__.__name__,
            section_name,
        )

    def stats(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        index: List[str],
        dimensions: List[str],
        dimensions_values: Dict[str, List[str]],
    ) -> Dict[str, Dict[str, Dict[str, float]]]:
        """Implement interface."""
        query = self._get_aggregation_query(index, dimensions, dimensions_values)
        buckets = []
        while True:
            ret = self._tina_reader.search_raw(
                start_ts=start_date,
                end_ts=end_date,
                query=query,
                limit=0,
            )
            buckets.extend(ret["aggregations"]["my_buckets"]["buckets"])
            after_key = ret["aggregations"]["my_buckets"].get("after_key")
            if not after_key:
                break
            query["aggs"]["my_buckets"]["composite"]["after"] = after_key
        return self._parse_aggregation_output(buckets, dimensions, dimensions_values)

    def group_by(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        index: List[str],
        dimensions: List[str],
    ) -> List[Dict[str, str]]:
        """Implement interface."""
        return self._tina_reader.aggregate(
            start_ts=start_date,
            end_ts=end_date,
            terms=dimensions + [index[1]],
            limit=None,
        )
