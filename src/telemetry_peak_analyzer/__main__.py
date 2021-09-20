# Copyright 2021 VMware, Inc.
# SPDX-License-Identifier: BSD-2
import argparse
import configparser
import datetime
import logging
import os
import sys
from typing import Optional
from typing import Tuple

import telemetry_peak_analyzer


def import_class(clazz_name: str) -> type:
    """
    Import the module and return the class.

    Example:
        > clazz = get_clazz_from_module("package.module.ClassName")
        > logging.debug("Instantiating %s instance...", clazz.__name__)
        > obj = clazz(conf)

    :param str clazz_name: class name in 'module.Class' form
    :rtype: type
    :return: the loadable type
    :raises ImportError: if the class name is not valid
    """
    if "." not in clazz_name:
        raise ImportError(f"Class '{clazz_name}' does not appear to be in module.Class form")

    try:
        only_clazz = clazz_name.split(".")[-1]
        only_module = ".".join(clazz_name.split(".")[:-1])
        mod = __import__(only_module, fromlist=[only_clazz])
        return getattr(mod, only_clazz)
    except AttributeError as ae:
        raise ImportError(f"Class '{clazz_name}' not found") from ae


def is_valid_date(date_str: str) -> datetime.date:
    """
    Validate a date and return a datetime object.

    :param str date_str: the datetime object as a string
    :rtype: datetime.date
    :return: the parsed date
    :raises ValueError: if the date is not valid
    """
    try:
        return datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(f"Not a valid date: '{date_str}'") from None


def parse_date_options(
    start_ts: datetime.date,
    end_ts: datetime.date,
    delta: int,
    delay: int,
) -> Tuple[datetime.datetime, datetime.datetime]:
    """
    Validate the date options.

    :param datetime.date start_ts: the start of the time interval
    :param datetime.date end_ts: the end of the time interval
    :param int delta: the length of the time interval
    :param int delay: the delay of the time interval
    :rtype: tuple[datetime.datetime, datetime.datetime]
    :return: the validated datetime objects
    :raises ValueError: if the provided interval is not valid
    """
    if start_ts and end_ts:
        if end_ts <= start_ts:
            raise ValueError(f"Invalid time interval {start_ts} - {end_ts}")
    else:
        utc_now = datetime.datetime.utcnow()
        end_ts = (utc_now - datetime.timedelta(days=delay)).date()
        start_ts = end_ts - datetime.timedelta(days=delta)
    return (
        datetime.datetime.combine(start_ts, datetime.datetime.min.time()),
        datetime.datetime.combine(end_ts, datetime.datetime.min.time()),
    )


def run(
    config: configparser.ConfigParser,
    analyzer_class: type,
    backend_class: type,
    backend_input: str,
    start_ts: datetime.datetime,
    end_ts: datetime.datetime,
    threshold: int,
    global_table_path: str,
    output_file_path: Optional[str] = None,
) -> int:
    """Run the telemetry peak analyzer."""
    logger = logging.getLogger(__name__)
    logger.info("Loading Peak Analyzer from %s to %s with t=%s", start_ts, end_ts, threshold)
    peak_analyzer = analyzer_class(
        conf=config,
        backend=backend_class(config, backend_input),
        start_ts=start_ts,
        end_ts=end_ts,
    )
    try:
        logger.info("Loading global tables from file '%s'", global_table_path)
        global_tables = peak_analyzer.get_global_tables_from_file(global_table_path)
    except IOError as ioe:
        logger.info("\tFailed: %s", str(ioe))
        logger.info("Loading global tables from the backend")
        global_tables = peak_analyzer.get_global_tables()

    logger.info("Loading local tables")
    local_tables = peak_analyzer.get_local_tables()
    peaks = peak_analyzer.get_peaks(global_tables, local_tables, threshold=threshold)

    logger.info("Getting peaks")
    for dimension_0 in peaks:
        for dimension_1 in peaks[dimension_0]:
            logger.info("TelemetryPeak(%s, %s)", dimension_0, dimension_1)
            peak = peaks[dimension_0][dimension_1]
            for k, v in peak._asdict().items():
                logger.info("\t%s: %s", k, round(v, 2))
    if output_file_path:
        logger.info("Saving output to: %s", output_file_path)
        peak_analyzer.save_to_json(peaks, output_file_path)

    logger.info("Refreshing global tables")
    global_tables = peak_analyzer.refresh_global_tables(global_tables, local_tables)

    logger.info("Saving global tables to '%s'", global_table_path)
    telemetry_peak_analyzer.save_to_json(global_tables, global_table_path)
    return 0


def parse_and_run_command():
    """
    Examples:
        # python -m telemetry_peak_analyzer \
            -b telemetry_peak_analyzer.backends.JsonBackend -n "~/data.*.json" \
            -s 2020-07-01 -e 2021-08-01 -t 10
        # python -m telemetry_peak_analyzer -c config.ini \
            -b telemetry_peak_analyzer.backends.tina.TinaBackend -n tina_backend \
            -s 2020-07-01 -e 2021-08-01 -t 10
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c",
        "--config-file",
        dest="config_file",
        default="./data/config.ini",
        type=str,
        help="read config from here",
    )
    # Time interval option 1: specify start and end datetime
    parser.add_argument(
        "-s",
        "--start-date",
        dest="start_date",
        default=None,
        type=is_valid_date,
        help="the start of the time interval in 'YYYY:mm:dd' format",
    )
    parser.add_argument(
        "-e",
        "--end-date",
        dest="end_date",
        default=None,
        type=is_valid_date,
        help="the end of the time interval in 'YYYY:mm:dd' format",
    )
    # Time interval option 2: specify the length and the delay of the time interval
    parser.add_argument(
        "-d",
        "--delta",
        dest="delta",
        default=1,
        type=int,
        help="the length of the time interval starting from now",
    )
    parser.add_argument(
        "-k",
        "--delay",
        dest="delay",
        default=0,
        type=int,
        help="the delay of the time interval expressed in days",
    )
    # Other options
    parser.add_argument(
        "-t",
        "--threshold",
        dest="threshold",
        default=None,
        type=int,
        help="the threshold used by the telemetry peak analyzer",
    )
    parser.add_argument(
        "-a",
        "--analyzer",
        dest="analyzer_class",
        default="telemetry_peak_analyzer.analyzers.FileTypePeakAnalyzer",
        type=import_class,
        help="the full class name of the analyzer used to process telemetry data",
    )
    parser.add_argument(
        "-b",
        "--backend-class",
        dest="backend_class",
        default="telemetry_peak_analyzer.backends.JsonBackend",
        type=import_class,
        help="the full class name of the backend used to read the telemetry from",
    )
    parser.add_argument(
        "-n",
        "--backend-input",
        dest="backend_input",
        required=True,
        type=str,
        help="the backend input, section name when reading remotely or a file for local input",
    )
    parser.add_argument(
        "-m",
        "--global-table",
        dest="global_table_path",
        default="global_table.json",
        type=str,
        help="the path to the global tables, either to load from, or to save to",
    )
    parser.add_argument(
        "-o",
        "--output-file",
        dest="output_file",
        default=None,
        type=str,
        help="the path to output file",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        dest="verbose",
        default=False,
        action="store_true",
        help="whether to be verbose",
    )

    # Parse options and init the logger
    args = parser.parse_args()
    conf = configparser.ConfigParser()
    conf.read(args.config_file)
    start_date, end_date = parse_date_options(
        args.start_date,
        args.end_date,
        args.delta,
        args.delay,
    )
    log_level = logging.DEBUG if args.verbose else logging.INFO
    telemetry_peak_analyzer.MemoryFootprintFormatter.configure_logging(log_level)

    # Run
    return run(
        conf,
        args.analyzer_class,
        args.backend_class,
        args.backend_input,
        start_date,
        end_date,
        args.threshold,
        os.path.abspath(args.global_table_path),
        os.path.abspath(args.output_file) if args.output_file else None,
    )


if __name__ == "__main__":
    sys.exit(parse_and_run_command())
