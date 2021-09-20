# Copyright 2021 VMware, Inc.
# SPDX-License-Identifier: BSD-2
import datetime
import json
import logging
import os
import resource
import sys
from typing import Dict
from typing import List
from typing import Union


def ms_to_datetime(milliseconds: int) -> datetime.datetime:
    """
    Convert a given amount of milliseconds to a datetime object.

    :param int milliseconds: number of milliseconds
    :rtype: datetime.datetime
    :return: the datetime object
    """
    return datetime.datetime.fromtimestamp(milliseconds / 1000)


def datetime_to_sec(timestamp: datetime.datetime) -> int:
    """
    Convert a given timestamp to seconds since the epoch.

    :param datetime.datetime timestamp: the datetime object
    :rtype: int
    """
    return int((timestamp - datetime.datetime.utcfromtimestamp(0)).total_seconds())


def datetime_to_ms(timestamp: datetime.datetime) -> int:
    """
    Convert a given timestamp to milliseconds since the epoch.

    :param datetime.datetime timestamp: the datetime object
    :rtype: int
    """
    return datetime_to_sec(timestamp) * 1000


def save_to_json(obj: Union[Dict, List], file_path: str) -> None:
    """
    Save python object.

    :param dict|list obj: the python object
    :param str file_path: the path where to save to
    """
    with open(file_path, "w") as f:
        json.dump(obj, f, indent=2 * " ", sort_keys=True, default=str)


class MemoryFootprintFormatter(logging.Formatter):
    """Special formatter keeping track how much memory is used."""

    _DEFAULT_FMT = "%(levelname)s -> [%(asctime)s] %(message)s"
    _DEFAULT_DATEFMT = "%Y-%m-%d %H:%M:%S"

    def __init__(
        self,
        fmt: str = _DEFAULT_FMT,
        datefmt: str = _DEFAULT_DATEFMT,
    ):
        """Override method."""
        super(MemoryFootprintFormatter, self).__init__(fmt, datefmt)

    @classmethod
    def configure_logging(cls, level: int) -> None:
        """
        Configure some sane defaults.

        :param int level: the debugging level.
        """
        handler = logging.StreamHandler()
        formatter = cls()
        handler.setFormatter(formatter)
        logging.root.addHandler(handler)
        logging.root.setLevel(level)

    @staticmethod
    def _read_procfs_memory(procfs_mem_key: str) -> float:
        """
        Read memory usage from the /proc filesystem.

        :param str procfs_mem_key: the key to search
        :rtype: float
        :return: the value of the related key
        """
        procfs_fn = os.path.join("/", "proc", str(os.getpid()), "status")
        procfs_stats_scale = {
            "kB": 1024.0,
            "mB": 1024.0 * 1024.0,
            "KB": 1024.0,
            "MB": 1024.0 * 1024.0,
        }
        with open(procfs_fn) as pf:
            pf_data = pf.read()
            # get VmKey line e.g. "VmRSS:  9999  kB\n ..."
            i = pf_data.index(procfs_mem_key)
            # remove white-spaces
            v = pf_data[i:].split(None, 3)
            if len(v) < 3:
                return 0.0
            # scale to the unit that was asked for
            return float(v[1]) * procfs_stats_scale[v[2]]

    @staticmethod
    def _get_memory_size() -> float:
        """
        Retrieve total memory usage in bytes from /proc filesystem.

        :rtype: float
        :return: the value of the memory size
        """
        return MemoryFootprintFormatter._read_procfs_memory("VmSize:")

    @staticmethod
    def _get_resident_memory_size() -> float:
        """
        Retrieve resident memory usage in bytes from /proc filesystem.

        :rtype: float
        :return: the value of the memory size
        """
        return MemoryFootprintFormatter._read_procfs_memory("VmRSS:")

    @classmethod
    def _get_memory_consumption(cls) -> float:
        """
        Get the current memory consumption in megabytes.

        :rtype: float
        :return: the memory consumption
        """
        try:
            # We get the actual memory footprint
            res_mem_size = MemoryFootprintFormatter._get_resident_memory_size()
        except IOError:
            # On OSX we do not have /proc so we fallback to resource
            if sys.platform == "darwin":
                # IMPORTANT: on OSX results are given in Bytes while on Linux are given in KBytes
                res_mem_size = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            else:
                # If we are not on Linux or on OSX just give up
                res_mem_size = 1024.0 * 1024.0
        res_mem_size /= 1024.0 * 1024.0
        return res_mem_size

    def format(self, record: logging.LogRecord) -> str:
        """Override."""
        # A record can be formatted multiple times by SimpleLogger.
        if not hasattr(record, "formatted"):
            record.msg = "[%04dmb] %s" % (self._get_memory_consumption(), record.msg)
            setattr(record, "formatted", True)
        return super(MemoryFootprintFormatter, self).format(record)
