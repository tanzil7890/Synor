# Local note catalog

This example turns Markdown notes into small JSON records. It uses only local
files, the local Synor engine, and Python’s standard library.

Run the local safety checks and preview from this directory:

```bash
synor doctor main.py --offline
synor plan main.py --offline
synor diff main.py --offline
synor update main.py --offline
synor explain main.py --offline
```

The checked-in `.env` keeps Synor's change ledger in `./synor.db`. No account,
API key, network service, or database server is required.

The first run writes `catalog/garden.json` and `catalog/deploy.json`. Run the
`update` command again and both work units are reused. Edit `notes/deploy.md`
and run it once more. Only that note is cataloged again.

Each controlled operation leaves a metadata-only manifest and audit stream
under `.synor/runs/`. They contain run and policy evidence, not note content.

Delete a note and its JSON record is removed because the missing work unit no
longer owns an outcome.

The example is deliberately small enough to answer the three Synor questions
without infrastructure:

1. What changed?
2. What work ran?
3. What outcome was repaired?
