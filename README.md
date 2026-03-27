# GeoNuclearData

This repository contains a worldwide nuclear reactor dataset published in JSON, CSV, and MySQL formats.

This fork is self-maintained from public World Nuclear Association pages instead of waiting for upstream refreshes. Plant-level data comes from the WNA Nuclear Power Plant Explorer, and reactor-level metadata is enriched from the public reactor detail pages.

### Version

Database version: **0.18.0** (**2026/03/27**)  
Dataset last updated in version: **0.18.0** (**2026/03/27**)

### Changelog

See [CHANGELOG](https://github.com/Nowalski/GeoNuclearData/blob/master/CHANGELOG.md) for release details.

### Data formats

Data is available in multiple formats (MySQL, JSON, and CSV).

### Quick database summary

| Status | Count |
|--------|------:|
| Planned | 125 |
| Under Construction | 78 |
| Operational | 438 |
| Shutdown | 224 |
| **Total** | **865** |

## Maintenance

The dataset can be refreshed with [`scripts/update_from_wna.py`](scripts/update_from_wna.py).

Requirements:

- `python`
- `requests`
- `beautifulsoup4`
- `playwright`
- Chromium installed for Playwright via `playwright install chromium`
- optional shortcut: `python -m pip install -r scripts/requirements-update.txt`

Example refresh command:

```bash
python scripts/update_from_wna.py --reactor-workers 16
```

Notes:

- Plant overlays are scraped from the visible WNA explorer pages.
- Reactor pages are fetched individually and validated before being merged into the dataset.
- Planned reactors without a public WNA detail page may not yet have `operator`, `reactor_type`, `reactor_model`, or `wna_url` values.
- A short maintainer workflow is documented in [`docs/MAINTENANCE.md`](docs/MAINTENANCE.md).

## Tables structure

### countries

- `code` - ISO 3166-1 alpha-2 country code
- `name` - country name in English

### nuclear_power_plant_status_type

- `id` - numeric id key
- `type` - nuclear reactor status

### nuclear_reactor_type

- `id` - numeric id key
- `type` - nuclear reactor type acronym
- `description` - nuclear reactor type long form

### nuclear_power_plants

- `id` - numeric id key
- `name` - canonical reactor name
- `display_name` - WNA display name shown in the explorer/detail page
- `plant_name` - nuclear plant/site name
- `latitude` - latitude in decimal format
- `longitude` - longitude in decimal format
- `country_code` - ISO 3166-1 alpha-2 country code
- `status_id` - nuclear reactor status id
- `reactor_type_id` - nuclear reactor type id
- `reactor_model` - reactor model
- `construction_start_at` - construction start date
- `operational_from` - commercial operation date or first grid connection if the commercial date is unavailable
- `operational_to` - permanent shutdown date
- `capacity` - design net capacity in MWe where available
- `operator` - reactor operator
- `source` - source of the information
- `last_updated_at` - date and time when the row was last refreshed
- `iaea_id` - legacy IAEA PRIS reactor id retained when a row matches previous data
- `wna_url` - source WNA reactor detail URL when available

## Notes

- Coordinates are approximate and inherited from public WNA map data.
- `source`, `last_updated_at`, `iaea_id`, and `wna_url` are maintenance-oriented fields.

## Usage

```sql
SELECT npp.id
    , npp.name
    , npp.display_name
    , npp.plant_name
    , npp.latitude
    , npp.longitude
    , c.name AS country
    , s.type AS status
    , r.type AS reactor_type
    , npp.reactor_model
    , npp.construction_start_at
    , npp.operational_from
    , npp.operational_to
    , npp.operator
FROM nuclear_power_plants npp
INNER JOIN countries AS c ON npp.country_code = c.code
INNER JOIN nuclear_power_plant_status_type AS s ON npp.status_id = s.id
LEFT OUTER JOIN nuclear_reactor_type AS r ON npp.reactor_type_id = r.id
ORDER BY npp.id;
```

## License

The GeoNuclearData database is made available under the Open Database License whose full text can be found at https://opendatacommons.org/licenses/odbl/1.0/.

Any rights in individual contents of the database are licensed under the Database Contents License whose full text can be found at https://opendatacommons.org/licenses/dbcl/1.0/.

## Sources

Countries data is taken from [Unicode Common Locale Data Repository](https://github.com/unicode-org/cldr-json/blob/main/cldr-json/cldr-localenames-full/main/en/territories.json).

Nuclear reactor data is taken from:

- [WNA Nuclear Power Plant Explorer](https://world-nuclear.org/information-library/facts-and-figures/nuclear-power-plant-explorer)
- [WNA Nuclear Reactor Database](https://world-nuclear.org/nuclear-reactor-database)
- linked [World Nuclear News](https://world-nuclear-news.org/) articles surfaced by the explorer when useful for discovery
