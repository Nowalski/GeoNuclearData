#!/usr/bin/env python3
"""Validate the generated GeoNuclearData exports without third-party packages."""

from __future__ import annotations

import csv
import json
import re
from datetime import date, datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

RAW_FIELDS = [
    "Id", "Name", "DisplayName", "PlantName", "Latitude", "Longitude",
    "CountryCode", "StatusId", "ReactorTypeId", "ReactorModel",
    "ConstructionStartAt", "OperationalFrom", "OperationalTo", "Capacity",
    "Operator", "Source", "LastUpdatedAt", "IAEAId", "WnaUrl",
]
DENORMALIZED_FIELDS = [
    "Id", "Name", "DisplayName", "PlantName", "Latitude", "Longitude",
    "Country", "CountryCode", "Status", "ReactorType", "ReactorModel",
    "ConstructionStartAt", "OperationalFrom", "OperationalTo", "Capacity",
    "Operator", "LastUpdatedAt", "Source", "IAEAId", "WnaUrl",
]


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def load_csv(path: Path):
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def validate_dates(row: dict) -> None:
    parsed: dict[str, date] = {}
    for field in ("ConstructionStartAt", "OperationalFrom", "OperationalTo"):
        value = row.get(field)
        if value:
            try:
                parsed[field] = date.fromisoformat(value)
            except ValueError as exc:
                raise ValueError(f"{row['Id']}: invalid {field}: {value}") from exc
    if "ConstructionStartAt" in parsed and "OperationalFrom" in parsed:
        require(
            parsed["ConstructionStartAt"] <= parsed["OperationalFrom"],
            f"{row['Id']}: construction starts after operation",
        )
    if "OperationalFrom" in parsed and "OperationalTo" in parsed:
        require(
            parsed["OperationalFrom"] <= parsed["OperationalTo"],
            f"{row['Id']}: shutdown precedes operation",
        )


def main() -> int:
    countries = load_json(DATA / "json/raw/1-countries.json")
    statuses = load_json(DATA / "json/raw/2-nuclear_power_plant_status_type.json")
    types = load_json(DATA / "json/raw/3-nuclear_reactor_type.json")
    raw = load_json(DATA / "json/raw/4-nuclear_power_plants.json")
    denormalized = load_json(DATA / "json/denormalized/nuclear_power_plants.json")
    raw_csv = load_csv(DATA / "csv/raw/4-nuclear_power_plants.csv")
    denormalized_csv = load_csv(DATA / "csv/denormalized/nuclear_power_plants.csv")
    datapackage = load_json(DATA / "csv/denormalized/datapackage.json")

    require(len(raw) == len(denormalized) == len(raw_csv) == len(denormalized_csv), "export row counts differ")
    ids = [row["Id"] for row in raw]
    require(len(ids) == len(set(ids)), "raw reactor IDs are not unique")
    require(ids == [int(row["Id"]) for row in raw_csv], "raw JSON/CSV ID order differs")
    require(ids == [int(row["Id"]) for row in denormalized_csv], "denormalized JSON/CSV ID order differs")

    country_codes = {row["Code"] for row in countries}
    country_names = {row["Code"]: row["Name"] for row in countries}
    status_names = {row["Id"]: row["Type"] for row in statuses}
    type_names = {row["Id"]: row["Type"] for row in types}
    require(len(country_codes) == len(countries), "country codes are not unique")
    require(len(status_names) == len(statuses), "status IDs are not unique")
    require(len(type_names) == len(types), "reactor type IDs are not unique")

    timestamps: set[str] = set()
    for row in raw:
        require(row["CountryCode"] in country_codes, f"{row['Id']}: unknown country code")
        require(row["StatusId"] in status_names, f"{row['Id']}: unknown status ID")
        if row["ReactorTypeId"] is not None:
            require(row["ReactorTypeId"] in type_names, f"{row['Id']}: unknown reactor type ID")
        require(-90 <= row["Latitude"] <= 90, f"{row['Id']}: latitude out of range")
        require(-180 <= row["Longitude"] <= 180, f"{row['Id']}: longitude out of range")
        require(row["Capacity"] is not None and row["Capacity"] >= 0, f"{row['Id']}: invalid capacity")
        require(row["WnaUrl"].startswith("https://"), f"{row['Id']}: invalid WNA URL")
        require(re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", row["LastUpdatedAt"]), f"{row['Id']}: invalid timestamp")
        datetime.fromisoformat(row["LastUpdatedAt"].replace("Z", "+00:00"))
        timestamps.add(row["LastUpdatedAt"])
        validate_dates(row)

    require(len(timestamps) == 1, "LastUpdatedAt is not consistent across raw rows")
    require(len({(row["CountryCode"], row["PlantName"]) for row in raw}) > 0, "no plant records found")
    require(
        [field["name"] for field in datapackage["resources"][0]["schema"]["fields"]] == DENORMALIZED_FIELDS,
        "datapackage schema does not match denormalized export",
    )
    require(list(denormalized_csv[0]) == DENORMALIZED_FIELDS, "denormalized CSV header is invalid")
    require(list(raw_csv[0]) == RAW_FIELDS, "raw CSV header is invalid")

    for raw_row, den_row in zip(raw, denormalized):
        require(den_row["Country"] == country_names[raw_row["CountryCode"]], f"{raw_row['Id']}: country mismatch")
        require(den_row["Status"] == status_names[raw_row["StatusId"]], f"{raw_row['Id']}: status mismatch")
        expected_type = type_names.get(raw_row["ReactorTypeId"])
        require(den_row["ReactorType"] == expected_type, f"{raw_row['Id']}: reactor type mismatch")

    sql = (DATA / "mysql/4-nuclear_power_plants.sql").read_text(encoding="utf-8")
    require(sql.count("INSERT INTO `nuclear_power_plants`") == len(raw), "MySQL row count differs")
    print(f"Validated {len(raw)} reactors across {len({(r['CountryCode'], r['PlantName']) for r in raw})} plants")
    print(f"Last updated: {next(iter(timestamps))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
