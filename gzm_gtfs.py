# SPDX-FileCopyrightText: 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import argparse
import csv
import logging
import re
import shutil
from collections import defaultdict
from collections.abc import Iterable
from datetime import date, datetime, timezone
from io import TextIOWrapper
from operator import itemgetter
from pathlib import Path
from zipfile import ZipFile

import requests
from impuls import App, LocalResource, PipelineOptions, Task, TaskRuntime
from impuls.errors import InputNotModified
from impuls.model import Date
from impuls.multi_file import IntermediateFeed, IntermediateFeedProvider, MultiFile
from impuls.tasks import ExecuteSQL, ExtendCalendarsFromPolishExceptions, LoadGTFS, SaveGTFS
from impuls.tools import polish_calendar_exceptions
from impuls.tools.color import text_color_for

GTFS_HEADERS = {
    "agency.txt": (
        "agency_id",
        "agency_name",
        "agency_url",
        "agency_timezone",
        "agency_phone",
        "agency_lang",
        "agency_fare_url",
        "agency_email",
    ),
    "calendar_dates.txt": (
        "service_id",
        "date",
        "exception_type",
    ),
    "feed_info.txt": (
        "feed_publisher_name",
        "feed_publisher_url",
        "feed_lang",
        "feed_start_date",
        "feed_end_date",
        "feed_contact_email",
        "feed_version",
    ),
    "routes.txt": (
        "route_id",
        "agency_id",
        "route_short_name",
        "route_long_name",
        "route_type",
        "route_color",
        "route_text_color",
    ),
    "shapes.txt": ("shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"),
    "stops.txt": ("stop_id", "stop_code", "stop_name", "stop_lat", "stop_lon"),
    "stop_times.txt": (
        "trip_id",
        "arrival_time",
        "departure_time",
        "stop_id",
        "stop_sequence",
        "stop_headsign",
        "pickup_type",
        "drop_off_type",
        "timepoint",
    ),
    "trips.txt": (
        "route_id",
        "service_id",
        "trip_id",
        "trip_headsign",
        "trip_short_name",
        "direction_id",
        "shape_id",
        "wheelchair_accessible",
        "block_id",
    ),
}


ROUTE_COLORS = [
    # Generic route-type based colors
    ("%", 0, "#d7006e"),
    ("%", 3, "#fcf299"),
    ("%", 11, "#88bb44"),
    # Route subtype specific colors (buses)
    ("T-%", 3, "#ffd403"),
    ("M%", 3, "#d7006e"),
    ("AP", 3, "#d7006e"),
    ("%N", 3, "#000000"),
    # Route specific colors (trolleybuses & trams)
    ("A", 11, "#21bbef"),
    ("B", 11, "#ea516d"),
    ("C", 11, "#85bd41"),
    ("D", 11, "#b94f98"),
    ("E", 11, "#f39200"),
    ("F", 11, "#f29fc5"),
    ("G", 11, "#4aa0af"),
    ("H", 11, "#fdc300"),
    ("0", 0, "#56be86"),
    ("1", 0, "#7cbcc7"),
    ("2", 0, "#517f87"),
    ("3", 0, "#addabd"),
    ("4", 0, "#7cbcc7"),
    ("5", 0, "#517f87"),
    ("6", 0, "#ffd403"),
    ("7", 0, "#f16d68"),
    ("9", 0, "#f6abad"),
    ("10", 0, "#ffd403"),
    ("11", 0, "#d8c497"),
    ("13", 0, "#4e7cbf"),
    ("14", 0, "#f69548"),
    ("15", 0, "#f16d68"),
    ("16", 0, "#85bae5"),
    ("19", 0, "#56be86"),
    ("20", 0, "#f16d68"),
    ("21", 0, "#7e71b4"),
    ("22", 0, "#7e71b4"),
    ("24", 0, "#c6afd4"),
    ("26", 0, "#f69548"),
    ("27", 0, "#c6afd4"),
    ("28", 0, "#7e71b4"),
    ("30", 0, "#517f87"),
    ("34", 0, "#f69548"),
    ("36", 0, "#f69548"),
    ("38", 0, "#b66987"),
    ("40", 0, "#f16d68"),
    ("41", 0, "#85bae5"),
    ("45", 0, "#f16d68"),
    ("46", 0, "#85bae5"),
]

UPPER_CASE_WORDS = ["GCR", "GPP", "II", "III", "NFZ", "PKP", "UG", "ZWM"]


class GZMFeedProvider(IntermediateFeedProvider[LocalResource]):
    def __init__(
        self,
        dir: Path = Path("_impuls_workspace/gzm_files"),
        force: bool = False,
        cache: bool = False,
    ) -> None:
        self.dir = dir
        self.force = force
        self.cache = cache

        self.pkg_date = Date(1, 1, 1)
        self.feeds = list[Path]()

        self.logger = logging.getLogger(type(self).__name__)

    def prepare(self) -> None:
        if self.feeds:
            return  # already prepared

        if self.cache:
            self.pkg_date, files = self._list_local_files()
            self.feeds = [i[0] for i in files]
            if not self.feeds:
                raise ValueError("cached run requested, but no input feeds are cached")
        else:
            self.feeds = self.fetch_input_files()
            if not self.feeds:
                raise InputNotModified

    def fetch_input_files(self) -> list[Path]:
        remote_pkg, remote_files = max(self._list_remote_files().items(), key=itemgetter(0))
        remote_mod_time = max(i[2] for i in remote_files)

        local_pkg, local_files = self._list_local_files()
        local_mod_time = max(
            (i[1] for i in local_files),
            default=datetime.min.replace(tzinfo=timezone.utc),
        )

        self.pkg_date = remote_pkg
        should_run = self.force or local_pkg != remote_pkg or remote_mod_time > local_mod_time
        if not should_run:
            return []

        return self.download_files(remote_files)

    def download_files(self, input: Iterable[tuple[str, str, datetime]]) -> list[Path]:
        files = list[Path]()
        tmp_dir = self.dir.with_name(f"{self.dir.name}.new")
        tmp_dir.mkdir(parents=True)
        try:
            for url, filename, _ in input:
                if "/" in filename or "\\" in filename:
                    raise ValueError(f"unsafe gtfs filename: {filename!r}")
                if not filename.endswith(".zip"):
                    raise ValueError(f"gtfs filename does not end in .zip: {filename!r}")
                self.download_file(url, tmp_dir / filename)
                files.append(self.dir / filename)

            try:
                shutil.rmtree(self.dir)
            except FileNotFoundError:
                pass
            tmp_dir.rename(self.dir)
        finally:
            try:
                shutil.rmtree(tmp_dir)
            except FileNotFoundError:
                pass
        return files

    def download_file(self, url: str, dst: Path) -> None:
        self.logger.debug("Downloading %s", url)
        with dst.open("wb") as f, requests.get(url, stream=True) as r:
            r.raise_for_status()
            for chunk in r.iter_content(chunk_size=None, decode_unicode=False):
                f.write(chunk)

    def _list_remote_files(self) -> defaultdict[date, list[tuple[str, str, datetime]]]:
        self.logger.info("Fetching remote resource list")
        files_by_pkg_date = defaultdict[date, list[tuple[str, str, datetime]]](list)
        with requests.get(
            "https://otwartedane.metropoliagzm.pl/api/3/action/package_show",
            params={"id": "rozklady-jazdy-i-lokalizacja-przystankow-gtfs-wersja-rozszerzona"},
        ) as r:
            r.raise_for_status()
            data = r.json()
            for resource in data["result"]["resources"]:
                if resource["mimetype"] != "application/zip":
                    continue

                url = resource["url"]

                name = resource["name"]
                pkg_date_match = re.search(r"([0-9]{4})\.([0-9]{2})\.([0-9]{2})", name)
                if not pkg_date_match:
                    raise ValueError(f"failed to extract pkg date from {name!r}")
                pkg_date = date(
                    int(pkg_date_match[1]), int(pkg_date_match[2]), int(pkg_date_match[3])
                )

                mod_time_str = resource["last_modified"]
                if not re.search(r"(Z|[+-][0-9][0-9]:?[0-9][0-9])$", mod_time_str):
                    # mod_time_str looks to have no timezone data, assume UTC (?)
                    mod_time_str = f"{mod_time_str}Z"
                mod_time = datetime.fromisoformat(mod_time_str)
                assert mod_time.tzinfo is not None

                files_by_pkg_date[pkg_date].append((url, name, mod_time))
        return files_by_pkg_date

    def _list_local_files(self) -> tuple[date, list[tuple[Path, datetime]]]:
        self.logger.info("Fetching local resource list")
        pkg_date = date.min
        files = list[tuple[Path, datetime]]()

        for file in self.dir.glob("*.zip"):
            m = re.search(r"([0-9]{4})\.([0-9]{2})\.([0-9]{2})", file.name)
            if not m:
                raise ValueError(f"failed to extract pkg date from {file.name!r}")
            file_pkg_date = date(int(m[1]), int(m[2]), int(m[3]))
            pkg_date = max(pkg_date, file_pkg_date)

            mod_time = datetime.fromtimestamp(file.stat().st_mtime, timezone.utc)
            files.append((file, mod_time))

        return pkg_date, files

    def needed(self) -> list[IntermediateFeed[LocalResource]]:
        self.prepare()
        return [self.feed_for_file(f) for f in self.feeds]

    def feed_for_file(self, file: Path) -> IntermediateFeed[LocalResource]:
        return IntermediateFeed(
            resource=LocalResource(file),
            resource_name=file.name,
            version=self._get_version(file.name),
            start_date=self._get_start_date(file),
        )

    @staticmethod
    def _get_version(filename: str) -> str:
        m = re.search(r"_([0-9]+)_[0-9]{4}\.zip", filename, re.IGNORECASE)
        if not m:
            raise ValueError(f"failed to extract feed_version from {filename!r}")
        return m[1]

    @staticmethod
    def _get_start_date(gtfs_zip: Path) -> Date:
        start_date = Date(9999, 12, 31)  # date.max
        with ZipFile(gtfs_zip, "r") as arch, arch.open("feed_info.txt") as f:
            for row in csv.DictReader(TextIOWrapper(f, "utf-8-sig", newline="")):
                start_date = min(start_date, Date.from_ymd_str(row["feed_start_date"]))
        return start_date


class UpdateFeedInfo(Task):
    def __init__(self, version: str) -> None:
        super().__init__()
        self.version = version

    def execute(self, r: TaskRuntime) -> None:
        with r.db.transaction():
            r.db.raw_execute(
                "UPDATE feed_info SET publisher_name = 'Mikołaj Kuranowski', "
                "publisher_url = 'https://mkuran.pl/gtfs/', version = ?",
                (self.version,),
            )


class UpdateRouteColors(Task):
    def execute(self, r: TaskRuntime) -> None:
        with r.db.transaction():
            r.db.raw_execute_many(
                "UPDATE routes SET color = ?, text_color = ? WHERE type = ? AND short_name LIKE ?",
                (
                    (color[1:], text_color_for(color), type, short_name_pattern)
                    for short_name_pattern, type, color in ROUTE_COLORS
                ),
            )


class UpdateRouteLongNames(Task):
    def execute(self, r: TaskRuntime) -> None:
        fixed = [
            (self.fix_long_name(i[1]), i[0])  # type: ignore
            for i in r.db.raw_execute("SELECT route_id, long_name FROM routes")
        ]
        with r.db.transaction():
            r.db.raw_execute_many("UPDATE routes SET long_name = ? WHERE route_id = ?", fixed)

    @staticmethod
    def fix_long_name(input: str) -> str:
        s = input.title()
        for word in UPPER_CASE_WORDS:
            s = re.sub(f"\\b{re.escape(word)}\\b", word, s, flags=re.I)
        return s


class GZMGTFS(App):
    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("-o", "--output", default="gzm.zip", help="path to output GTFS file")

    def prepare(
        self,
        args: argparse.Namespace,
        options: PipelineOptions,
    ) -> MultiFile[LocalResource]:
        provider = GZMFeedProvider(force=options.force_run, cache=options.from_cache)
        return MultiFile(
            options=options,
            intermediate_provider=provider,
            intermediate_pipeline_tasks_factory=lambda feed: [
                LoadGTFS(feed.resource_name),
            ],
            final_pipeline_tasks_factory=lambda _: [
                UpdateFeedInfo(provider.pkg_date.strftime("%Y.%m.%d")),
                ExecuteSQL(
                    statement=(
                        "UPDATE routes SET short_name = substr(short_name, 2) "
                        "WHERE type = 0 AND short_name LIKE 'T%'"
                    ),
                    task_name="UpdateTramRouteShortName",
                ),
                UpdateRouteColors(),
                UpdateRouteLongNames(),
                ExtendCalendarsFromPolishExceptions(
                    resource_name="calendar_exceptions.csv",
                    region=polish_calendar_exceptions.PolishRegion.SLASKIE,
                ),
                SaveGTFS(headers=GTFS_HEADERS, target=args.output),
            ],
            additional_resources={
                "calendar_exceptions.csv": polish_calendar_exceptions.RESOURCE,
            },
        )


if __name__ == "__main__":
    GZMGTFS().run()
