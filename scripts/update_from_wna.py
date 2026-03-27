#!/usr/bin/env python3
"""Refresh GeoNuclearData from World Nuclear Association public pages.

This updater uses two public WNA surfaces:

1. The Nuclear Power Plant Explorer page for plant-level overlays.
2. Public reactor detail pages for reactor-level metadata.

No private API is required. The small TSV fetched by the explorer is used only
to discover the current reactor/plant index and public detail URLs.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import re
import sys
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

EXPLORER_URL = (
    "https://world-nuclear.org/information-library/facts-and-figures/"
    "nuclear-power-plant-explorer?plant={plant}"
)
EXPLORER_INDEX_URL = "https://wna-data-worker.alec-mitchell.workers.dev"
USER_AGENT = "GeoNuclearData-Updater/0.18.0 (+https://github.com/Nowalski/GeoNuclearData)"

VERSION = "0.18.0"
RUN_DATE = datetime.now(UTC)
RUN_DATE_ISO = RUN_DATE.replace(microsecond=0).isoformat().replace("+00:00", "Z")
RUN_DATE_SQL = RUN_DATE.strftime("%Y-%m-%d %H:%M:%S")
RUN_DATE_README = RUN_DATE.strftime("%Y/%m/%d")

RAW_COUNTRIES_PATH = DATA_DIR / "json" / "raw" / "1-countries.json"
RAW_STATUSES_PATH = DATA_DIR / "json" / "raw" / "2-nuclear_power_plant_status_type.json"
RAW_TYPES_PATH = DATA_DIR / "json" / "raw" / "3-nuclear_reactor_type.json"
RAW_REACTORS_PATH = DATA_DIR / "json" / "raw" / "4-nuclear_power_plants.json"

STATUS_TYPES = [
    {"Id": 0, "Type": "Unknown"},
    {"Id": 1, "Type": "Planned"},
    {"Id": 2, "Type": "Under Construction"},
    {"Id": 3, "Type": "Operational"},
    {"Id": 4, "Type": "Suspended Operation"},
    {"Id": 5, "Type": "Shutdown"},
    {"Id": 6, "Type": "Unfinished"},
    {"Id": 7, "Type": "Never Built"},
    {"Id": 8, "Type": "Suspended Construction"},
    {"Id": 9, "Type": "Cancelled Construction"},
    {"Id": 10, "Type": "Never Commissioned"},
    {"Id": 11, "Type": "Decommissioning Completed"},
]

WNA_STATUS_MAP = {
    "planned": (1, "Planned"),
    "under construction": (2, "Under Construction"),
    "operable": (3, "Operational"),
    "permanent shutdown": (5, "Shutdown"),
}

COUNTRY_ALIASES = {
    "czechrepublic": "Czechia",
    "southkorea": "South Korea",
    "northkorea": "North Korea",
    "taiwan": "Taiwan",
    "turkey": "Türkiye",
    "unitedstatesofamerica": "United States",
    "uae": "United Arab Emirates",
    "turkiye": "Türkiye",
}

RAW_FIELD_ORDER = [
    "Id",
    "Name",
    "DisplayName",
    "PlantName",
    "Latitude",
    "Longitude",
    "CountryCode",
    "StatusId",
    "ReactorTypeId",
    "ReactorModel",
    "ConstructionStartAt",
    "OperationalFrom",
    "OperationalTo",
    "Capacity",
    "Operator",
    "Source",
    "LastUpdatedAt",
    "IAEAId",
    "WnaUrl",
]

DENORMALIZED_FIELD_ORDER = [
    "Id",
    "Name",
    "DisplayName",
    "PlantName",
    "Latitude",
    "Longitude",
    "Country",
    "CountryCode",
    "Status",
    "ReactorType",
    "ReactorModel",
    "ConstructionStartAt",
    "OperationalFrom",
    "OperationalTo",
    "Capacity",
    "Operator",
    "LastUpdatedAt",
    "Source",
    "IAEAId",
    "WnaUrl",
]


@dataclass(frozen=True)
class PlantTarget:
    plant_name: str
    country: str


def log(message: str) -> None:
    sys.stdout.write(f"{message}\n")
    sys.stdout.flush()


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def write_csv(path: Path, rows: list[dict[str, Any]], field_order: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=field_order)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: csv_value(row.get(field)) for field in field_order})


def csv_value(value: Any) -> Any:
    return "" if value is None else value


def normalize_key(value: str | None) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", value)
    normalized = normalized.encode("ascii", "ignore").decode("ascii")
    normalized = normalized.lower()
    normalized = re.sub(r"[^a-z0-9]+", "", normalized)
    return normalized


def clean_spaces(value: str | None) -> str | None:
    if value is None:
        return None
    value = re.sub(r"\s+", " ", value).strip()
    return value or None


def parse_date(value: str | None) -> str | None:
    text = clean_spaces(value)
    if not text or text == "—":
        return None
    if text == "—":
        return None
    for fmt in ("%A, %d %B %Y", "%A, %e %B %Y", "%d %B %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    match = re.search(r"(\d{1,2}\s+[A-Za-z]+\s+\d{4})", text)
    if match:
        for fmt in ("%d %B %Y", "%e %B %Y"):
            try:
                return datetime.strptime(match.group(1), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
    return None


def parse_float(value: str | None) -> float | None:
    text = clean_spaces(value)
    if not text:
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", text.replace(",", ""))
    return float(match.group(0)) if match else None


def parse_int(value: str | None) -> int | None:
    number = parse_float(value)
    if number is None:
        return None
    return int(round(number))


def status_from_wna(status_text: str | None) -> tuple[int, str]:
    mapped = WNA_STATUS_MAP.get((status_text or "").strip().lower())
    return mapped if mapped else (0, "Unknown")


def type_code_from_text(raw_type: str | None) -> tuple[str | None, str | None]:
    text = clean_spaces(raw_type)
    if not text:
        return None, None

    match = re.search(r"^(.*?)\s*\((?:or\s+)?([A-Z0-9-]+)\)$", text)
    if match:
        return match.group(2).strip(), clean_spaces(match.group(1))

    match = re.search(r"\(([A-Z0-9-]+)\)$", text)
    if match:
        acronym = match.group(1).strip()
        description = clean_spaces(text[: match.start()].strip())
        return acronym, description

    return None, text


def infer_type_code(description: str) -> str:
    words = re.findall(r"[A-Za-z0-9]+", description)
    acronym = "".join(word[0].upper() for word in words if word)
    return (acronym[:12] or "UNK").upper()


def slugify_url_part(value: str | None) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", value)
    normalized = normalized.encode("ascii", "ignore").decode("ascii")
    normalized = normalized.lower()
    normalized = normalized.replace("&", " and ")
    normalized = re.sub(r"[^a-z0-9]+", "-", normalized)
    return normalized.strip("-")


def sql_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def sql_value(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return f"{value:.6f}"
    return f"'{sql_escape(str(value))}'"


def load_existing_reactors() -> tuple[list[dict[str, Any]], dict[tuple[str, str], dict[str, Any]]]:
    rows = load_json(RAW_REACTORS_PATH)
    index: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        key = (row["CountryCode"], normalize_key(row["Name"]))
        index[key] = row
    return rows, index


def load_countries() -> tuple[list[dict[str, Any]], dict[str, str], dict[str, str]]:
    rows = load_json(RAW_COUNTRIES_PATH)
    by_name: dict[str, str] = {}
    by_code: dict[str, str] = {}
    for row in rows:
        code = row["Code"]
        name = row["Name"]
        by_name[normalize_key(name)] = code
        by_code[code] = name
    return rows, by_name, by_code


def resolve_country_code(country_name: str, country_map: dict[str, str]) -> str:
    key = normalize_key(country_name)
    if key in country_map:
        return country_map[key]
    alias = COUNTRY_ALIASES.get(key)
    if alias and normalize_key(alias) in country_map:
        return country_map[normalize_key(alias)]
    raise KeyError(f"Unknown country mapping for {country_name!r}")


def fetch_explorer_index(session: requests.Session) -> list[dict[str, str]]:
    log(f"Fetching current explorer index from {EXPLORER_INDEX_URL}")
    response = session.get(EXPLORER_INDEX_URL, timeout=60)
    response.raise_for_status()
    reader = csv.DictReader(io.StringIO(response.text), delimiter="\t")
    rows = [dict(row) for row in reader]
    if not rows:
        raise RuntimeError("Explorer index returned no rows")
    return rows


def scrape_plant_overlay(page, plant: PlantTarget) -> dict[str, Any]:
    url = EXPLORER_URL.format(plant=quote(plant.plant_name, safe=""))
    page.goto(url, wait_until="networkidle", timeout=120_000)
    page.wait_for_function(
        """expected => {
            const overlay = document.querySelector('#plantInfoOverlay');
            const title = document.querySelector('#plantInfoName');
            const style = overlay ? (overlay.getAttribute('style') || '') : '';
            return style.includes('display: flex') && title && title.textContent.trim().length > 0;
        }""",
        arg=plant.plant_name,
        timeout=30_000,
    )
    page.wait_for_timeout(500)

    data = page.evaluate(
        """() => {
            const text = (selector) => {
                const node = document.querySelector(selector);
                return node ? node.textContent.trim() : null;
            };
            return {
                country: text('#plantInfoCountry'),
                plant_name: text('#plantInfoName'),
                status: text('#plantInfoStatusBadge'),
                reactor_count: text('#plantInfoReactorCount'),
                capacity_gw: text('#plantInfoCapacity'),
                first_grid_year: text('#plantInfoFirstGrid'),
                reactors: Array.from(document.querySelectorAll('#plantInfoReactorsList .plant-info-reactor-item')).map((item) => ({
                    display_name: item.querySelector('.plant-info-reactor-name')?.textContent?.trim() || null,
                    details: item.querySelector('.plant-info-reactor-details')?.textContent?.trim() || null,
                    capacity_gw: item.querySelector('.plant-info-reactor-capacity')?.textContent?.trim() || null,
                    url: item.querySelector('a.plant-info-reactor-linkout')?.href || null
                }))
            };
        }"""
    )
    return data


def parse_reactor_detail(html: str, url: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")

    def select_text(selector: str) -> str | None:
        node = soup.select_one(selector)
        return clean_spaces(node.get_text(" ", strip=True)) if node else None

    kv: dict[str, str] = {}
    for row in soup.select(".reactor_db_top_tables_wrapper table tbody tr"):
        heading = row.find("th")
        value = row.find("td")
        if heading and value:
            kv[clean_spaces(heading.get_text(" ", strip=True)) or ""] = (
                clean_spaces(value.get_text(" ", strip=True)) or ""
            )

    reactor_name_input = soup.select_one("#reactorName")
    reactor_country_input = soup.select_one("#reactorCountry")
    latitude_input = soup.select_one("#Latitude")
    longitude_input = soup.select_one("#Longitude")

    notes_block = soup.select_one(".col-12.col-xl-12.dark_grey.regular.mt-3")
    notes_text = clean_spaces(notes_block.get_text(" ", strip=True)) if notes_block else None
    aliases: list[str] = []
    if notes_text and notes_text.lower().startswith("also known as "):
        aliases = [clean_spaces(item) for item in notes_text[14:].split(",")]
        aliases = [item for item in aliases if item]

    return {
        "country": clean_spaces(
            reactor_country_input.get("data-reactorlocation") if reactor_country_input else None
        )
        or select_text(".news_box_pretitle"),
        "reactor_name": clean_spaces(
            reactor_name_input.get("data-reactorname") if reactor_name_input else None
        )
        or select_text("h1.news_box_title"),
        "display_name": select_text("h1.news_box_title"),
        "status": select_text(".news_box_date"),
        "latitude": parse_float(latitude_input.get("value") if latitude_input else None),
        "longitude": parse_float(longitude_input.get("value") if longitude_input else None),
        "details": kv,
        "aliases": aliases,
        "url": url,
    }


def reactor_detail_candidates(index_row: dict[str, str], overlay_url: str | None = None) -> list[str]:
    seen: set[str] = set()
    candidates: list[str] = []

    def add(url: str | None) -> None:
        if url and url not in seen:
            seen.add(url)
            candidates.append(url)

    base = "https://world-nuclear.org/nuclear-reactor-database/details/"
    add(overlay_url)
    add(index_row.get("URL"))
    add(base + slugify_url_part(index_row.get("Reactor Name")))
    add(base + slugify_url_part(index_row.get("Display name")))
    plant_slug = slugify_url_part(index_row.get("Plant name"))
    unit_match = re.search(r"(\d+)$", (index_row.get("Display name") or "").strip())
    if plant_slug:
        add(base + plant_slug)
        if unit_match:
            unit = unit_match.group(1)
            add(base + f"{plant_slug}-{unit}")
            add(base + f"{plant_slug}-project-{unit}")
    return candidates


def detail_matches_index_row(index_row: dict[str, str], detail: dict[str, Any]) -> bool:
    expected_country = normalize_key(index_row.get("Country"))
    detail_country = normalize_key(detail.get("country"))
    if expected_country and detail_country and expected_country != detail_country:
        return False

    expected_status_id, _ = status_from_wna(index_row.get("Status"))
    detail_status_id, _ = status_from_wna(detail.get("status"))
    if expected_status_id and detail_status_id and expected_status_id != detail_status_id:
        return False

    expected_names = {
        normalize_key(index_row.get("Reactor Name")),
        normalize_key(index_row.get("Display name")),
    }
    expected_names.discard("")

    detail_names = {
        normalize_key(detail.get("reactor_name")),
        normalize_key(detail.get("display_name")),
        *(normalize_key(alias) for alias in detail.get("aliases", [])),
    }
    detail_names.discard("")

    if expected_names and detail_names and expected_names.isdisjoint(detail_names):
        return False

    return True


def fetch_reactor_detail(
    index_row: dict[str, str],
    overlay_url: str | None = None,
    timeout: float = 60,
) -> dict[str, Any]:
    headers = {"User-Agent": USER_AGENT}
    last_error: Exception | None = None
    for candidate in reactor_detail_candidates(index_row, overlay_url=overlay_url):
        response = requests.get(candidate, timeout=timeout, headers=headers)
        if response.status_code == 404:
            continue
        try:
            response.raise_for_status()
        except Exception as exc:  # pragma: no cover - network errors
            last_error = exc
            continue
        detail = parse_reactor_detail(response.text, response.url)
        if detail_matches_index_row(index_row, detail):
            return detail

    if last_error:
        raise last_error
    raise requests.HTTPError(f"No working reactor detail URL found for {index_row['Display name']}")


def fallback_reactor_detail(index_row: dict[str, str]) -> dict[str, Any]:
    return {
        "country": index_row.get("Country"),
        "reactor_name": index_row.get("Reactor Name"),
        "display_name": index_row.get("Display name"),
        "status": index_row.get("Status"),
        "latitude": parse_float(index_row.get("Latitude")),
        "longitude": parse_float(index_row.get("Longitude")),
        "details": {},
        "aliases": [],
        "url": index_row.get("URL"),
    }


def row_key(index_row: dict[str, str]) -> tuple[str, str, str, str]:
    return (
        index_row["Country"],
        index_row["Plant name"],
        index_row["Display name"],
        index_row["Reactor Name"],
    )


def ensure_type(
    raw_type: str | None,
    types_by_code: dict[str, dict[str, Any]],
    types_by_desc: dict[str, dict[str, Any]],
) -> tuple[int | None, str | None]:
    if not raw_type:
        return None, None

    code, description = type_code_from_text(raw_type)
    code = code or (infer_type_code(description or raw_type))
    description = description or raw_type

    existing = types_by_code.get(code) or types_by_desc.get(normalize_key(description))
    if existing:
        return existing["Id"], existing["Type"]

    next_id = max((entry["Id"] for entry in types_by_code.values()), default=0) + 1
    record = {"Id": next_id, "Type": code, "Description": description}
    types_by_code[code] = record
    types_by_desc[normalize_key(description)] = record
    return record["Id"], record["Type"]


def pick_existing_match(
    existing_by_key: dict[tuple[str, str], dict[str, Any]],
    country_code: str,
    *candidates: str | None,
) -> dict[str, Any] | None:
    for candidate in candidates:
        key = (country_code, normalize_key(candidate))
        if key in existing_by_key:
            return existing_by_key[key]
    return None


def build_rows(
    index_rows: list[dict[str, str]],
    plant_overlays: dict[tuple[str, str], dict[str, Any]],
    reactor_details: dict[str, dict[str, Any]],
    countries_by_name: dict[str, str],
    country_names_by_code: dict[str, str],
    existing_by_key: dict[tuple[str, str], dict[str, Any]],
    types_by_code: dict[str, dict[str, Any]],
    types_by_desc: dict[str, dict[str, Any]],
    next_new_id: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    raw_rows: list[dict[str, Any]] = []
    used_ids: set[int] = set()

    def assign_id(existing: dict[str, Any] | None) -> int:
        nonlocal next_new_id
        if existing and existing.get("Id") not in used_ids:
            used_ids.add(existing["Id"])
            return existing["Id"]
        while next_new_id in used_ids:
            next_new_id += 1
        assigned = next_new_id
        used_ids.add(assigned)
        next_new_id += 1
        return assigned

    for index_row in sorted(
        index_rows,
        key=lambda row: (
            row["Country"],
            row["Plant name"],
            row["Display name"],
            row["Reactor Name"],
        ),
    ):
        detail = reactor_details[row_key(index_row)]
        country_code = resolve_country_code(index_row["Country"], countries_by_name)
        overlay = plant_overlays[(index_row["Plant name"], index_row["Country"])]
        status_id, _ = status_from_wna(detail.get("status") or index_row.get("Status"))
        reactor_type_id, _ = ensure_type(
            detail["details"].get("Reactor Type"),
            types_by_code,
            types_by_desc,
        )

        existing = pick_existing_match(
            existing_by_key,
            country_code,
            detail.get("reactor_name"),
            index_row.get("Reactor Name"),
            index_row.get("Display name"),
            *detail.get("aliases", []),
        )

        operational_from = parse_date(detail["details"].get("Commercial Operation"))
        if not operational_from:
            operational_from = parse_date(detail["details"].get("First Grid Connection"))

        raw_row = {
            "Id": assign_id(existing),
            "Name": detail.get("reactor_name") or index_row["Reactor Name"],
            "DisplayName": index_row.get("Display name") or detail.get("display_name"),
            "PlantName": overlay.get("plant_name") or index_row["Plant name"],
            "Latitude": detail.get("latitude")
            if detail.get("latitude") is not None
            else parse_float(index_row.get("Latitude")),
            "Longitude": detail.get("longitude")
            if detail.get("longitude") is not None
            else parse_float(index_row.get("Longitude")),
            "CountryCode": country_code,
            "StatusId": status_id,
            "ReactorTypeId": reactor_type_id,
            "ReactorModel": detail["details"].get("Model"),
            "ConstructionStartAt": parse_date(detail["details"].get("Construction Start")),
            "OperationalFrom": operational_from,
            "OperationalTo": parse_date(detail["details"].get("Permanent Shutdown")),
            "Capacity": parse_int(
                detail["details"].get("Design Net Capacity")
                or detail["details"].get("Capacity Net")
                or index_row.get("Gross Capacity")
            ),
            "Operator": detail["details"].get("Operator"),
            "Source": "WNA Explorer / Reactor Database",
            "LastUpdatedAt": RUN_DATE_ISO,
            "IAEAId": existing.get("IAEAId") if existing else None,
            "WnaUrl": detail["url"] or index_row.get("URL"),
        }
        raw_rows.append(raw_row)

    raw_rows.sort(key=lambda row: row["Id"])

    statuses_by_id = {entry["Id"]: entry["Type"] for entry in STATUS_TYPES}
    denormalized_rows: list[dict[str, Any]] = []
    for row in raw_rows:
        type_record = (
            next(
                (entry for entry in types_by_code.values() if entry["Id"] == row["ReactorTypeId"]),
                None,
            )
            if row["ReactorTypeId"] is not None
            else None
        )
        denormalized_rows.append(
            {
                "Id": row["Id"],
                "Name": row["Name"],
                "DisplayName": row["DisplayName"],
                "PlantName": row["PlantName"],
                "Latitude": row["Latitude"],
                "Longitude": row["Longitude"],
                "Country": country_names_by_code[row["CountryCode"]],
                "CountryCode": row["CountryCode"],
                "Status": statuses_by_id[row["StatusId"]],
                "ReactorType": type_record["Type"] if type_record else None,
                "ReactorModel": row["ReactorModel"],
                "ConstructionStartAt": row["ConstructionStartAt"],
                "OperationalFrom": row["OperationalFrom"],
                "OperationalTo": row["OperationalTo"],
                "Capacity": row["Capacity"],
                "Operator": row["Operator"],
                "LastUpdatedAt": row["LastUpdatedAt"],
                "Source": row["Source"],
                "IAEAId": row["IAEAId"],
                "WnaUrl": row["WnaUrl"],
            }
        )

    return raw_rows, denormalized_rows, next_new_id


def build_types_tables() -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    rows = load_json(RAW_TYPES_PATH)
    by_code = {row["Type"]: dict(row) for row in rows}
    by_desc = {normalize_key(row["Description"]): dict(row) for row in rows}
    return by_code, by_desc


def write_datapackage() -> None:
    payload = {
        "profile": "tabular-data-package",
        "resources": [
            {
                "name": "nuclear-power-plants",
                "path": "nuclear_power_plants.csv",
                "profile": "tabular-data-resource",
                "schema": {
                    "fields": [
                        {"name": "Id", "type": "integer", "format": "default", "title": "ID"},
                        {"name": "Name", "type": "string", "format": "default", "title": "Reactor Name"},
                        {"name": "DisplayName", "type": "string", "format": "default", "title": "Display Name"},
                        {"name": "PlantName", "type": "string", "format": "default", "title": "Plant Name"},
                        {"name": "Latitude", "type": "number", "format": "default", "title": "Latitude"},
                        {"name": "Longitude", "type": "number", "format": "default", "title": "Longitude"},
                        {"name": "Country", "type": "string", "format": "default", "title": "Country"},
                        {"name": "CountryCode", "type": "string", "format": "default", "title": "Country Code"},
                        {"name": "Status", "type": "string", "format": "default", "title": "Status"},
                        {"name": "ReactorType", "type": "string", "format": "default", "title": "Reactor Type"},
                        {"name": "ReactorModel", "type": "string", "format": "default", "title": "Reactor Model"},
                        {"name": "ConstructionStartAt", "type": "date", "format": "%Y-%m-%d", "title": "Construction Start Date"},
                        {"name": "OperationalFrom", "type": "date", "format": "%Y-%m-%d", "title": "Commercial Operation Date"},
                        {"name": "OperationalTo", "type": "date", "format": "%Y-%m-%d", "title": "Permanent Shutdown Date"},
                        {"name": "Capacity", "type": "integer", "format": "default", "title": "Design Net Capacity (MWe)"},
                        {"name": "Operator", "type": "string", "format": "default", "title": "Operator"},
                        {"name": "LastUpdatedAt", "type": "datetime", "format": "%Y-%m-%dT%H:%M:%SZ", "title": "Last Updated Datetime"},
                        {"name": "Source", "type": "string", "format": "default", "title": "Data Source"},
                        {"name": "IAEAId", "type": "integer", "format": "default", "title": "IAEA ID"},
                        {"name": "WnaUrl", "type": "string", "format": "uri", "title": "WNA URL"},
                    ]
                },
            }
        ],
        "name": "nuclear-power-plants",
        "title": "Nuclear Power Plants Database",
        "description": "This repository contains a database with information about nuclear power plants worldwide.",
        "homepage": "https://github.com/Nowalski/GeoNuclearData",
        "licenses": [
            {
                "name": "ODbL-1.0",
                "title": "Open Data Commons Open Database License 1.0",
                "path": "http://www.opendefinition.org/licenses/odc-odbl",
            }
        ],
        "version": VERSION,
        "contributors": [
            {"title": "Cristian Stoica", "role": "author"},
            {"title": "Alex Nowalski", "role": "maintainer"},
        ],
    }
    write_json(DATA_DIR / "csv" / "denormalized" / "datapackage.json", payload)


def write_lookup_sql(path: Path, table_name: str, columns: list[str], rows: list[dict[str, Any]]) -> None:
    lines = [
        "SET NAMES utf8mb4;",
        "SET FOREIGN_KEY_CHECKS = 0;",
        "",
        f"DROP TABLE IF EXISTS `{table_name}`;",
    ]

    if table_name == "countries":
        lines.extend(
            [
                "CREATE TABLE `countries` (",
                "  `code` char(2) NOT NULL,",
                "  `name` varchar(64) NOT NULL,",
                "  PRIMARY KEY (`code`)",
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
            ]
        )
    elif table_name == "nuclear_power_plant_status_type":
        lines.extend(
            [
                "CREATE TABLE `nuclear_power_plant_status_type` (",
                "  `id` tinyint UNSIGNED NOT NULL,",
                "  `type` varchar(64) NOT NULL,",
                "  PRIMARY KEY (`id`)",
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
            ]
        )
    elif table_name == "nuclear_reactor_type":
        lines.extend(
            [
                "CREATE TABLE `nuclear_reactor_type` (",
                "  `id` smallint UNSIGNED NOT NULL,",
                "  `type` varchar(32) NOT NULL,",
                "  `description` varchar(128) NOT NULL,",
                "  PRIMARY KEY (`id`)",
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
            ]
        )

    lines.append("")
    for row in rows:
        values = ", ".join(sql_value(row[column]) for column in columns)
        lines.append(f"INSERT INTO `{table_name}` VALUES ({values});")
    lines.append("")
    lines.append("SET FOREIGN_KEY_CHECKS = 1;")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_reactors_sql(path: Path, rows: list[dict[str, Any]]) -> None:
    lines = [
        "SET NAMES utf8mb4;",
        "SET FOREIGN_KEY_CHECKS = 0;",
        "",
        "DROP TABLE IF EXISTS `nuclear_power_plants`;",
        "CREATE TABLE `nuclear_power_plants` (",
        "  `id` smallint UNSIGNED NOT NULL,",
        "  `name` varchar(64) NOT NULL,",
        "  `display_name` varchar(128) DEFAULT NULL,",
        "  `plant_name` varchar(128) DEFAULT NULL,",
        "  `latitude` decimal(10, 6) DEFAULT NULL,",
        "  `longitude` decimal(10, 6) DEFAULT NULL,",
        "  `country_code` char(2) NOT NULL,",
        "  `status_id` tinyint UNSIGNED NOT NULL,",
        "  `reactor_type_id` smallint UNSIGNED DEFAULT NULL,",
        "  `reactor_model` varchar(64) DEFAULT NULL,",
        "  `construction_start_at` date DEFAULT NULL,",
        "  `operational_from` date DEFAULT NULL,",
        "  `operational_to` date DEFAULT NULL,",
        "  `capacity` int UNSIGNED DEFAULT NULL COMMENT 'design net capacity in MWe',",
        "  `operator` varchar(128) DEFAULT NULL,",
        "  `source` varchar(64) DEFAULT NULL,",
        "  `last_updated_at` datetime DEFAULT NULL,",
        "  `iaea_id` int DEFAULT NULL,",
        "  `wna_url` varchar(255) DEFAULT NULL,",
        "  PRIMARY KEY (`id`),",
        "  UNIQUE KEY `idx_name_country` (`name`, `country_code`),",
        "  KEY `idx_country_code` (`country_code`),",
        "  KEY `idx_status_id` (`status_id`),",
        "  KEY `idx_reactor_type_id` (`reactor_type_id`)",
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
        "",
    ]
    columns = [
        "Id",
        "Name",
        "DisplayName",
        "PlantName",
        "Latitude",
        "Longitude",
        "CountryCode",
        "StatusId",
        "ReactorTypeId",
        "ReactorModel",
        "ConstructionStartAt",
        "OperationalFrom",
        "OperationalTo",
        "Capacity",
        "Operator",
        "Source",
        "LastUpdatedAt",
        "IAEAId",
        "WnaUrl",
    ]
    for row in rows:
        sql_row = dict(row)
        sql_row["LastUpdatedAt"] = RUN_DATE_SQL
        values = ", ".join(sql_value(sql_row[column]) for column in columns)
        lines.append(f"INSERT INTO `nuclear_power_plants` VALUES ({values});")
    lines.append("")
    lines.append("SET FOREIGN_KEY_CHECKS = 1;")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def save_outputs(
    countries_rows: list[dict[str, Any]],
    types_rows: list[dict[str, Any]],
    raw_rows: list[dict[str, Any]],
    denormalized_rows: list[dict[str, Any]],
) -> None:
    json_raw_dir = DATA_DIR / "json" / "raw"
    json_denormalized_dir = DATA_DIR / "json" / "denormalized"
    csv_raw_dir = DATA_DIR / "csv" / "raw"
    csv_denormalized_dir = DATA_DIR / "csv" / "denormalized"

    write_json(json_raw_dir / "1-countries.json", countries_rows)
    write_json(json_raw_dir / "2-nuclear_power_plant_status_type.json", STATUS_TYPES)
    write_json(json_raw_dir / "3-nuclear_reactor_type.json", types_rows)
    write_json(json_raw_dir / "4-nuclear_power_plants.json", raw_rows)
    write_json(json_denormalized_dir / "nuclear_power_plants.json", denormalized_rows)

    write_csv(csv_raw_dir / "1-countries.csv", countries_rows, ["Code", "Name"])
    write_csv(csv_raw_dir / "2-nuclear_power_plant_status_type.csv", STATUS_TYPES, ["Id", "Type"])
    write_csv(
        csv_raw_dir / "3-nuclear_reactor_type.csv",
        types_rows,
        ["Id", "Type", "Description"],
    )
    write_csv(csv_raw_dir / "4-nuclear_power_plants.csv", raw_rows, RAW_FIELD_ORDER)
    write_csv(
        csv_denormalized_dir / "nuclear_power_plants.csv",
        denormalized_rows,
        DENORMALIZED_FIELD_ORDER,
    )
    write_datapackage()

    write_lookup_sql(DATA_DIR / "mysql" / "1-countries.sql", "countries", ["Code", "Name"], countries_rows)
    write_lookup_sql(
        DATA_DIR / "mysql" / "2-nuclear_power_plant_status_type.sql",
        "nuclear_power_plant_status_type",
        ["Id", "Type"],
        STATUS_TYPES,
    )
    write_lookup_sql(
        DATA_DIR / "mysql" / "3-nuclear_reactor_type.sql",
        "nuclear_reactor_type",
        ["Id", "Type", "Description"],
        types_rows,
    )
    write_reactors_sql(DATA_DIR / "mysql" / "4-nuclear_power_plants.sql", raw_rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit-plants", type=int, default=None, help="Only scrape the first N plants")
    parser.add_argument(
        "--sleep-plant",
        type=float,
        default=0.0,
        help="Seconds to wait between plant overlay scrapes",
    )
    parser.add_argument(
        "--sleep-reactor",
        type=float,
        default=0.0,
        help="Optional delay applied inside each reactor worker after a successful fetch",
    )
    parser.add_argument(
        "--reactor-workers",
        type=int,
        default=10,
        help="Number of concurrent reactor detail fetch workers",
    )
    args = parser.parse_args()

    countries_rows, countries_by_name, country_names_by_code = load_countries()
    existing_rows, existing_by_key = load_existing_reactors()
    next_new_id = max(row["Id"] for row in existing_rows) + 1
    types_by_code, types_by_desc = build_types_tables()

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    index_rows = fetch_explorer_index(session)
    plants = sorted(
        {PlantTarget(row["Plant name"], row["Country"]) for row in index_rows},
        key=lambda item: (item.country, item.plant_name),
    )
    if args.limit_plants is not None:
        plants = plants[: args.limit_plants]
        allowed = {(plant.plant_name, plant.country) for plant in plants}
        index_rows = [row for row in index_rows if (row["Plant name"], row["Country"]) in allowed]

    log(f"Scraping {len(plants)} plant overlays and {len(index_rows)} reactor pages")

    plant_overlays: dict[tuple[str, str], dict[str, Any]] = {}
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 2200})
        for idx, plant in enumerate(plants, start=1):
            log(f"[plant {idx}/{len(plants)}] {plant.country} :: {plant.plant_name}")
            try:
                overlay = scrape_plant_overlay(page, plant)
                plant_overlays[(plant.plant_name, plant.country)] = overlay
            except PlaywrightTimeoutError as exc:
                raise RuntimeError(f"Timed out scraping plant overlay for {plant.plant_name}") from exc
            time.sleep(args.sleep_plant)
        browser.close()

    reactor_details: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    failed_reactors: list[str] = []
    overlay_detail_urls: dict[tuple[str, str, str], str] = {}
    for (plant_name, country), overlay in plant_overlays.items():
        for reactor in overlay.get("reactors", []):
            display_name = reactor.get("display_name")
            reactor_url = reactor.get("url")
            if display_name and reactor_url:
                overlay_detail_urls[(plant_name, country, normalize_key(display_name))] = reactor_url

    with ThreadPoolExecutor(max_workers=max(1, args.reactor_workers)) as pool:
        future_map = {}
        for row in index_rows:
            overlay_url = overlay_detail_urls.get(
                (row["Plant name"], row["Country"], normalize_key(row["Display name"]))
            )
            future = pool.submit(fetch_reactor_detail, row, overlay_url)
            future_map[future] = row
        for idx, future in enumerate(as_completed(future_map), start=1):
            row = future_map[future]
            try:
                detail = future.result()
            except Exception as exc:
                failed_reactors.append(f"{row['Display name']}: {exc}")
                detail = fallback_reactor_detail(row)
            reactor_details[row_key(row)] = detail
            log(f"[reactor {idx}/{len(index_rows)}] {row['Display name']}")
            if args.sleep_reactor > 0:
                time.sleep(args.sleep_reactor)

    raw_rows, denormalized_rows, _ = build_rows(
        index_rows=index_rows,
        plant_overlays=plant_overlays,
        reactor_details=reactor_details,
        countries_by_name=countries_by_name,
        country_names_by_code=country_names_by_code,
        existing_by_key=existing_by_key,
        types_by_code=types_by_code,
        types_by_desc=types_by_desc,
        next_new_id=next_new_id,
    )

    types_rows = sorted(types_by_code.values(), key=lambda row: row["Id"])
    save_outputs(countries_rows, types_rows, raw_rows, denormalized_rows)

    counts = Counter(row["Status"] for row in denormalized_rows)
    log("")
    log("Refresh complete")
    log(f"  Version: {VERSION}")
    log(f"  Reactors: {len(denormalized_rows)}")
    log(f"  Plants: {len({(row['PlantName'], row['Country']) for row in denormalized_rows})}")
    if failed_reactors:
        log(f"  Detail fallbacks: {len(failed_reactors)}")
    for status, count in sorted(counts.items()):
        log(f"  {status}: {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
