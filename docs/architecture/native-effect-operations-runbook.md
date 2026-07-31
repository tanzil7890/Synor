# Native effect retention and downgrade runbook

This runbook covers the native effect keyspace introduced by schema version 3.
It applies to Synor app databases that may contain metadata-only revocation
effects. It does not replace the backup, retention, or incident procedures for
the Phase 2 control-plane `StateStore`.

The safe default is indefinite evidence retention. Synor never compacts
completed native effects automatically.

## Operator commands

The commands operate on the LMDB database directory passed to `--db`. Archive
paths and downgrade output directories must not already exist and must resolve
outside the source database. Synor resolves parent symlinks before accepting a
downgrade staging path.

Export one app without changing its database:

```bash
synor native-effects export \
  --db /srv/synor/state \
  --app-name search-index \
  --output /secure/synor/search-index-native-effects.json
```

Archive and compact completed history through an explicit UTC cutoff:

```bash
synor native-effects compact \
  --db /srv/synor/state \
  --app-name search-index \
  --completed-before 2026-01-01T00:00:00Z \
  --archive /secure/synor/search-index-before-2026.json \
  --confirm-compaction
```

Prepare a separate database copy for a binary that predates native schema
version 3:

```bash
synor native-effects prepare-downgrade \
  --db /srv/synor/state \
  --output-db /srv/synor/state-pre-native \
  --archive /secure/synor/state-pre-native-effects.json \
  --confirm-downgrade
```

Use `--json` on any command for a versioned, machine-readable result. The
commands print only counts, paths, and archive hashes. Record-level metadata is
written to the private archive.

## Archive contract

Archives use `synor.native-effects.archive` schema version 1. Each archive
contains:

- the native schema version for each app;
- bounded action and evidence IDs;
- operation, source generation, controlled cause, policy, status, timestamps,
  attempt count, and controlled error code;
- source and target SHA-256 digests;
- an opaque tracking fingerprint;
- a canonical SHA-256 digest over the archived app records;
- the exact evidence IDs selected for compaction, when applicable.

Archives do not contain target payloads, source content, raw locators,
principals, credentials, or remote error text. Bounded IDs are not a PII
classifier, so store archives as sensitive operational evidence. Synor creates
archive files with mode `0600`, fsyncs their contents, and publishes them
without overwriting an existing path. POSIX hosts enforce the mode bits and
fsync the parent directory after publication. On Windows, protect the archive
parent with an appropriate ACL; Python does not expose directory fsync there,
so directory-entry durability follows the filesystem and volume guarantees.

Back up the Phase 2 control-plane `StateStore` separately. Its cases, receipts,
suppression state, and receipt heads are not duplicated into this native LMDB
archive.

## Retention and compaction

Compaction is opt-in and follows archive-first ordering:

1. Synor reads one consistent metadata snapshot.
2. It writes and fsyncs the complete archive.
3. It submits only the completed evidence IDs recorded in that archive.
4. One LMDB transaction revalidates every selected record.
5. The transaction retains every record referenced by an ordinary lineage or
   cleanup-obligation cursor.
6. The transaction atomically deletes only unreferenced completed records.

Records with an unknown zero timestamp remain indefinitely. Pending, verified,
failed, and blocked records are never eligible. The current record for every
ordinary lineage and cleanup obligation remains even when it is older than the
cutoff. Repeating the same command is safe: previously deleted IDs are reported
as `already_absent`, and cursor heads remain `protected`.

If archive publication fails, the database is unchanged. If the process exits
after archive publication but before compaction, the archive remains and the
database is unchanged. LMDB either commits the complete deletion set or none
of it.

Retain archives according to the deployment's evidence policy. Removing an
archive is a separate operator decision and is not performed by Synor.

## Downgrade procedure

A native schema v3 database is a one-way boundary for the original database.
Never point a pre-native binary at that database. The downgrade command creates
a new copy and leaves the source untouched.

Before preparing the copy:

1. Stop schedulers, app workers, live controllers, and operator retries that
   can write this environment.
2. Wait for in-flight updates to finish.
3. Resolve every pending, verified, failed, or blocked native effect with the
   current binary.
4. Resolve every child cleanup tombstone, including legacy-unverified
   tombstones.
5. Back up the source database and the Phase 2 control-plane `StateStore`.
6. Keep serving suppression active throughout the change.

The command then:

1. Acquires an OS-backed exclusive environment-operation lease. Every normal
   app update and drop holds the corresponding shared lease, so existing and
   newly starting app operations cannot cross the snapshot boundary.
2. Creates an LMDB-consistent compacted snapshot with `mdb_env_copy`.
3. Opens only that staging copy with the current binary.
4. Validates native schema and cursor integrity for every copied app.
5. Refuses the copy if any effect is unresolved or any tombstone exists.
6. Captures the exact native metadata from the copied snapshot.
7. Removes only the native schema marker, effect records, obligation cursors,
   lineage cursors, and live-generation sequencer from the copy.
8. Fsyncs the copied LMDB environment.
9. Releases the source environment lease after the immutable copy is ready.
10. Writes and fsyncs the external archive.
11. Adds `DOWNGRADE_READY.json` with the archive SHA-256 and removal counts.
12. Publishes the output directory only after the archive exists.

The source database is never stripped or mutated. A failed preparation removes
its hidden staging directory and does not publish the requested output path.
The lease depends on operating-system file-lock semantics. Use a local
filesystem or a shared filesystem whose lock behavior has been validated for
every participating host.

## Cutover verification

Verify the archive and readiness manifest before starting an older binary:

```bash
shasum -a 256 /secure/synor/state-pre-native-effects.json
cat /srv/synor/state-pre-native/DOWNGRADE_READY.json
```

The hashes must match. Start the intended older binary against the copy in an
isolated environment and run the app's compatibility lifecycle before routing
production work to it. At minimum, run an update, drop, and second update with
the deployment's actual providers registered.

Do not delete or overwrite the current database after cutover. Retain it until
the rollback window and evidence-retention period have both expired.

## Failure handling

| State | Operator action |
|---|---|
| Export command fails | Fix the reported database or filesystem problem. No database mutation occurred. |
| Compaction archive exists but command failed | Inspect the error. Re-run with a new archive path after correcting it. Do not edit the first archive. |
| Downgrade reports unresolved effects | Resume with the current binary, converge or retry the effects, then prepare a new copy. |
| Downgrade reports child tombstones | Resume cleanup with the current binary. Do not discard the tombstones. |
| Hidden downgrade staging directory remains after host failure | Keep the source database. Remove the unpublished staging directory and run the command again with new output and archive paths. |
| A hidden `*.synor-publish.lock` remains after host failure | Confirm no downgrade process is running, keep any published output for inspection, then remove only the stale publish lock. Use new output and archive paths for another run. |
| Output database exists without a matching readiness hash | Do not deploy it. Keep the source and create a new copy. |
| Older-binary validation fails | Stop the older binary. Keep serving from or restore the current deployment. Never modify the source to make the older reader accept it. |

## Validated compatibility drill

The repository test suite verifies archive-selected compaction, cursor-head
retention, idempotency, source preservation, copied operational-state
preservation, native-key removal, and refusal on unresolved effects or any
tombstone.

The recorded manual drill used the pre-native macOS wheel built from revision
`63df53f605a552547cc016ef879d7cdf582e76e8`. A current database with one
completed native effect was copied by `prepare-downgrade`; the archive retained
that record; the copy removed one schema marker, one effect, and one lineage
cursor; and the pre-native binary completed `update -> drop -> update` against
the copied app.

This drill proves database compatibility at that supported boundary. It does
not prove physical erasure at a connector, cross-region disaster recovery, or
sudden-power-loss behavior of the underlying storage device.
