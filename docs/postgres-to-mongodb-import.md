# PostgreSQL to MongoDB one-time import

This procedure is deliberately non-destructive. It never changes or deletes PostgreSQL rows.

## Before running

1. Take a separate PostgreSQL backup using your provider's backup/export facility.
2. Provision MongoDB and configure `MONGODB_URI` and `MONGODB_DB_NAME`.
3. Set `POSTGRES_SOURCE_URL` to the old PostgreSQL connection string. Do not put it in `MONGODB_URI`.
4. Install dependencies with `npm install` (the repository should commit the generated lockfile before production use).
5. Stop bot writes during the final import/cutover window, or take a consistent PostgreSQL snapshot. The importer does not provide cross-database transactional consistency.

## Dry run

```bash
npm run migrate:postgres:dry
```

The default mode reads every expected legacy table and reports source row counts. It does not write to MongoDB.

## Apply

Only after reviewing the dry-run output and taking backups:

```bash
npm run migrate:postgres:apply
```

The importer creates `postgres_legacy_<table>` collections and inserts source rows without overwriting existing archive documents. It is safe to rerun for rows already imported. PostgreSQL is read-only from this script's perspective.

Rows from `temp_data` and `cache_data` are additionally projected into `key_value_store` using insert-if-absent semantics. Existing MongoDB keys are never overwritten.

## Important limitation — not a cutover-ready migration

The old PostgreSQL schema contains structured tables (guilds, users, economy, levels, tickets, birthdays, etc.). Their original rows are retained in the `postgres_legacy_*` archive collections, but this importer does not yet convert those tables into the bot's canonical key/value document model. That conversion needs explicit per-feature mapping and validation against the current application readers/writers.

Do not point production traffic exclusively at the new MongoDB database until those structured records have been mapped, imported, and verified. Compare per-table source counts with archive counts and test economy balances, guild configuration, tickets, levels, birthdays, giveaways and verification records in a staging deployment.

The script does not create the MongoDB schema version record; run the normal explicit schema command separately after MongoDB is provisioned.
