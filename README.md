![GitHub tag (latest SemVer)](https://img.shields.io/github/tag/vmware-labs/telemetry-peak-analyzer)
![GitHub](https://img.shields.io/pypi/l/telemetry-peak-analyzer)
![GitHub issues](https://img.shields.io/github/issues/vmware-labs/telemetry-peak-analyzer)

# Telemetry Peak Analyzer

## Overview

Telemetry Peak Analyzer is a framework to analyze and detect peaks on telemetry data with multiple
dimensions, indexes, and attributes. The analyzer detects meaningful peaks based on statistical
measurements computed over a short _local window_ and a longer _global window_ of telemetry data:

- _Local window_ - a short time data window in which we want to detect peaks of a given attribute
or dimension, e.g., file type. During the detection process, the analyzer generates a local
statistics table (LST) with all the necessary statistical measurements.

- _Global window_ - a historical long time data window which serves as a global benchmark to
determine if a detected peak within the _local window_ is meaningful. During the detection process,
it will generate (or update) a global statistics table (GST) with all the necessary statistical
measurements.

Telemetry data is dynamic, therefore the global benchmark as reflected by GST needs to be updated
over time. To make the global benchmark adaptive, we use a sliding window mechanism which allows
us to quickly update the new GST using previous GST and LST.

*Note*: this implementation is a generalization of a research tool that was tailored to detect waves
of malicious files sharing the same file type; to fully generalize terms and components, the source
code relies on the following terms to describe different parts of the telemetry feed:
- `index`: a tuple of attributes used to uniquely identify a telemetry data record.
- `dimensions`: the attributes used to decompose a time-series into independent and orthogonal
time-series.

Such generalization is not perfect (for example, the current implementation does not support more
than two dimensions) and some backends have obvious limitations; things will improve as the
analyzer supports more types of telemetry data.

## Try it out

### Build & Run

This package can be installed via pip, just run `pip install telemetry-peak-analyzer` or
`pip install -e .`.

If you want to install the dependencies required by the `tina` backend (a custom backend based
on Elasticsearch used internally) you should append the `[tina]` extra option; you might need to
use double quotes when doing a dev install, i.e., `pip install -e ".[tina]"`; note that a valid
configuration file might be required. See `data/config.ini.template` for an example.

Extra backends might require private dependencies; if that is the case, remember to select the
internal index server using the `-i` option; if you require access, contact one of the maintainers.

### Scripts

This package includes a console script ready to be used. Examples:

* `python -m telemetry_peak_analyzer -b
telemetry_peak_analyzer.backends.JsonBackend -n "./data/telemetry_example_*" -t 10`:
in this example the peak analyzer reads from some local files using the JSON backend
(note the double quotes) and sets the threshold to 10; note that when `-t` is specified, it
will overwrite any suggested global threshold defined in GST.
* `python -m telemetry_peak_analyzer -c config.ini -b
telemetry_peak_analyzer.backends.tina.TinaBackend -n tina_nlemea -d 2`:
in this example the peak analyzer reads from Tina from the last 2 days of data, using the
configuration file `config.ini`, and the section `tina_nlemea` to know how to connect to the
backend.

### Test
There are a number of JSON files in the  `data` directory for test using the JSON backend.
Note that all the test files have been completely anonymized, to the point that even file hashes
do not refer to actual files anymore.

As mentioned above, the analyzer detects peaks based on statistical measurements of both a
_local window_ and a _global window_. In the detailed example, the process comprises two steps.

1) `python -m telemetry_peak_analyzer -n ./data/telemetry_example_3.json -s 2020-11-01 –e 2020-11-04`

This step generates an initial GST table as global benchmark from the defined initial
_global window_, as specified by `-s` and `-e` options in the command. This step is only required
the first time the analyzer is executed. Subsequent runs will update the GST using previously
computed GST and LST tables.

Expected output:

```
test@localhost telemetry-peak-analyzer % python -m telemetry_peak_analyzer -n ./data/telemetry_example_3.json -s 2020-11-01 -e 2020-11-04
INFO -> [2021-09-15 12:00:11] [0010mb] Loading Peak Analyzer from 2020-11-01 00:00:00 to 2020-11-04 00:00:00 with t=None
INFO -> [2021-09-15 12:00:11] [0010mb] Loading backend 'JsonBackend'
INFO -> [2021-09-15 12:00:11] [0010mb] Loaded files:
INFO -> [2021-09-15 12:00:11] [0010mb]  /Users/test/telemetry-peak-analyzer/data/telemetry_example_3.json
INFO -> [2021-09-15 12:00:11] [0010mb] Loading analyzer 'FileTypePeakAnalyzer' with backend 'JsonBackend'
INFO -> [2021-09-15 12:00:11] [0010mb] Loading global tables from file '/Users/test/telemetry-peak-analyzer/global_table.json'
INFO -> [2021-09-15 12:00:11] [0010mb]  Failed: [Errno 2] No such file or directory: '/Users/test/telemetry-peak-analyzer/global_table.json'
INFO -> [2021-09-15 12:00:11] [0010mb] Loading global tables from the backend
INFO -> [2021-09-15 12:00:12] [0012mb] Loading local tables
INFO -> [2021-09-15 12:00:12] [0013mb] Getting peaks
INFO -> [2021-09-15 12:00:12] [0013mb] Refreshing global tables
INFO -> [2021-09-15 12:00:12] [0013mb] Saving global tables to '/Users/test/telemetry-peak-analyzer/global_table.json'
```

As the output shows, the process creates a JSON file `global_table.json` which is the initial
GST table containing the global statistics.

2) `python -m telemetry_peak_analyzer -n ./data/telemetry_example_3.json -s 2020-11-04 –e 2020-11-05`

This step will finally detect peaks from a _local window_ (as specified by `-s` and `-e` options)
by leveraging the statistics in the GST and LST tables. This run will also update the GST (ideally,
in production, you want to execute this second command on a daily basis to minimize the data to be
processed).

Expected output:

```
test@localhost telemetry-peak-analyzer % python -m telemetry_peak_analyzer -n ./data/telemetry_example_3.json -s 2020-11-04 -e 2020-11-05
INFO -> [2021-09-15 12:00:46] [0010mb] Loading Peak Analyzer from 2020-11-04 00:00:00 to 2020-11-05 00:00:00 with t=None
INFO -> [2021-09-15 12:00:46] [0010mb] Loading backend 'JsonBackend'
INFO -> [2021-09-15 12:00:46] [0010mb] Loaded files:
INFO -> [2021-09-15 12:00:46] [0010mb]  /Users/test/telemetry-peak-analyzer/data/telemetry_example_3.json
INFO -> [2021-09-15 12:00:46] [0010mb] Loading analyzer 'FileTypePeakAnalyzer' with backend 'JsonBackend'
INFO -> [2021-09-15 12:00:46] [0010mb] Loading global tables from file '/Users/test/telemetry-peak-analyzer/global_table.json'
INFO -> [2021-09-15 12:00:46] [0010mb] Loading local tables
INFO -> [2021-09-15 12:00:46] [0015mb] Getting peaks
INFO -> [2021-09-15 12:00:46] [0015mb] TelemetryPeak(malicious, ZipArchiveFile)
INFO -> [2021-09-15 12:00:46] [0015mb]  sub_count: 11083
INFO -> [2021-09-15 12:00:46] [0015mb]  samp_count: 3028
INFO -> [2021-09-15 12:00:46] [0015mb]  samp_sub_count_max: 426
INFO -> [2021-09-15 12:00:46] [0015mb]  samp_sub_count_mean: 3.66
INFO -> [2021-09-15 12:00:46] [0015mb]  samp_sub_count_std: 11.54
INFO -> [2021-09-15 12:00:46] [0015mb]  samp_sub_ratio: 0.04
INFO -> [2021-09-15 12:00:46] [0015mb]  global_samp_sub_count_max: 2
INFO -> [2021-09-15 12:00:46] [0015mb]  global_threshold_suggested: 629
INFO -> [2021-09-15 12:00:46] [0015mb] Refreshing global tables
INFO -> [2021-09-15 12:00:46] [0015mb] Saving global tables to '/Users/test/telemetry-peak-analyzer/global_table.json'
```

As the output shows, it loads the GST generated from the 1st step, and successfully detects a
ZipArchiveFile-based peak within the _local window_, and prints out some key statistical
measurements generated during the detection process.

At the end of the process, the GST table gets updated.


## Contributing

The telemetry-peak-analyzer project team welcomes contributions from the community. Before you
start working with telemetry-peak-analyzer, please read our
[Developer Certificate of Origin](https://cla.vmware.com/dco). All contributions to this repository
must be signed as described on that page. Your signature certifies that you wrote the patch or
have the right to pass it on as an open-source patch. For more detailed information,
refer to [CONTRIBUTING.md](CONTRIBUTING.md).

## Development

Create the virtual env:

`python3 -m venv venv`

Activate the virtual env:

`source ./venv/bin/activate`

Install `tox`:

`pip install tox`

Run tests:

`tox`

Due to a bug in `tox` if you update the dependencies in `setup.cfg` the environments will not be
re-created, leading to errors when running the tests
(see https://github.com/tox-dev/tox/issues/93).
As workaround, pass the `--recreate` flag after updating the dependencies.

Before committing, install the package in dev mode (needed by `pylint`) following the instructions
detailed in the `Build & Run` section.

Then install `pylint` and `pre-commit`:

`pip install pylint pre-commit`

Install the hook:

`pre-commit install`

If you want to run pre-commit on all files use the following command:

`pre-commit run --all-files`

## License
[BSD 2-Clause](https://spdx.org/licenses/BSD-2-Clause.html)
