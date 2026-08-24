# Maintenance

This fork is maintained from public World Nuclear Association pages.

## One-time setup

```bash
python -m pip install -r scripts/requirements-update.txt
python -m playwright install chromium
```

## Refresh the dataset

```bash
python scripts/update_from_wna.py --reactor-workers 16
```

## Quick checks

```bash
python -m py_compile scripts/update_from_wna.py
git status --short
```

Expected current release after the 2026-08-24 refresh:

- `867` reactors
- `349` plants
- `441` operational
- `79` under construction
- `123` planned
- `224` shutdown

Run the format and relationship checks after a refresh:

```bash
python scripts/validate_data.py
```

## Known limitations

- Many planned reactors do not yet have a public WNA reactor detail page.
- Those planned rows can legitimately have missing `operator`, `reactor_type`, `reactor_model`, and `wna_url` fields.
- Coordinates come from visible WNA map data and should be treated as approximate.

## Suggested push flow

```bash
git add .
git commit -m "Release GeoNuclearData 0.19.0"
git push origin master
```
