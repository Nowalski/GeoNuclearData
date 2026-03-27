# Maintenance

This fork is maintained from public World Nuclear Association pages.

## One-time setup

```bash
python -m pip install -r scripts/requirements-update.txt
playwright install chromium
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

Expected current release after the 2026-03-27 refresh:

- `865` reactors
- `337` plants
- `438` operational
- `78` under construction
- `125` planned
- `224` shutdown

## Known limitations

- Many planned reactors do not yet have a public WNA reactor detail page.
- Those planned rows can legitimately have missing `operator`, `reactor_type`, `reactor_model`, and `wna_url` fields.
- Coordinates come from visible WNA map data and should be treated as approximate.

## Suggested push flow

```bash
git add .
git commit -m "Release GeoNuclearData 0.18.0"
git push origin master
```
