import 'dotenv/config';
import pg from 'pg';
import { MongoClient } from 'mongodb';

/**
 * One-way, non-destructive PostgreSQL archive/import.
 *
 * Default mode is a dry run. Pass --apply to write to MongoDB.
 * Every source row is retained in a per-table archive collection with its
 * original columns. temp_data/cache_data rows are additionally copied into
 * key_value_store because those are already key/value records used by the bot.
 *
 * This deliberately does NOT drop, update, or delete PostgreSQL records or
 * overwrite an existing MongoDB document. Structured-table application
 * projections require a separately reviewed mapping before cutover.
 */
const APPLY = process.argv.includes('--apply');
const TABLES = [
    'guilds',
    'users',
    'guild_users',
    'birthdays',
    'giveaways',
    'ticket_data',
    'afk_status',
    'welcome_configs',
    'leveling_configs',
    'user_levels',
    'economy',
    'verification_audit',
    'invite_tracking',
    'application_roles',
    'temp_data',
    'cache_data',
];
const BATCH_SIZE = 500;

function required(name, value) {
    if (!value || !String(value).trim()) {
        throw new Error(`${name} is required. Set it in the environment; no source database was contacted.`);
    }
    return String(value).trim();
}

function stableId(row) {
    // Preserve original primary-key fields; deterministic JSON identity makes reruns idempotent.
    return JSON.stringify(Object.keys(row).sort().map(key => [key, row[key] instanceof Date ? row[key].toISOString() : row[key]]));
}

function parseJsonValue(value) {
    if (typeof value !== 'string') return value;
    try { return JSON.parse(value); } catch { return value; }
}

async function main() {
    const sourceUrl = required('POSTGRES_SOURCE_URL', process.env.POSTGRES_SOURCE_URL);
    const mongoUri = required('MONGODB_URI', process.env.MONGODB_URI);
    const mongoDbName = (process.env.MONGODB_DB_NAME || new URL(mongoUri).pathname.split('/').filter(Boolean)[0] || 'titanbot').trim();

    const source = new pg.Client({
        connectionString: sourceUrl,
        ssl: process.env.POSTGRES_SOURCE_SSL === 'false' ? false : { rejectUnauthorized: false },
        connectionTimeoutMillis: 15000,
        statement_timeout: 120000,
        application_name: 'krew-bot-postgres-migration',
    });
    const mongo = new MongoClient(mongoUri, { appName: 'KrewBotPostgresMigration', serverSelectionTimeoutMS: 15000 });

    const report = {
        mode: APPLY ? 'apply' : 'dry-run',
        source: 'PostgreSQL',
        destination: mongoDbName,
        tables: {},
        archivedRows: 0,
        keyValueRows: 0,
        inserted: 0,
        alreadyPresent: 0,
        errors: [],
    };

    try {
        await source.connect();
        await mongo.connect();
        const db = mongo.db(mongoDbName);
        await db.command({ ping: 1 });

        // Validate all source tables before writing anything.
        const available = await source.query(
            'SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema() AND table_type = $1',
            ['BASE TABLE'],
        );
        const availableTables = new Set(available.rows.map(row => row.table_name));
        const missing = TABLES.filter(table => !availableTables.has(table));
        if (missing.length) {
            throw new Error(`Source database is missing expected tables: ${missing.join(', ')}. No import was started.`);
        }

        for (const table of TABLES) {
            const result = await source.query(`SELECT * FROM "${table}"`);
            const rows = result.rows;
            const archive = db.collection(`postgres_legacy_${table}`);
            let inserted = 0;
            let alreadyPresent = 0;
            let kvInserted = 0;

            for (let offset = 0; offset < rows.length; offset += BATCH_SIZE) {
                const batch = rows.slice(offset, offset + BATCH_SIZE);
                const archiveOps = batch.map(row => ({
                    updateOne: {
                        filter: { _id: stableId(row) },
                        update: { $setOnInsert: { sourceTable: table, importedAt: new Date(), row } },
                        upsert: true,
                    },
                }));

                if (APPLY && archiveOps.length) {
                    const result = await archive.bulkWrite(archiveOps, { ordered: false });
                    inserted += result.upsertedCount || 0;
                    alreadyPresent += batch.length - (result.upsertedCount || 0);
                }

                if (APPLY && (table === 'temp_data' || table === 'cache_data')) {
                    const kv = db.collection('key_value_store');
                    for (const row of batch) {
                        if (typeof row.key !== 'string' || !row.key) {
                            report.errors.push(`${table}: skipped row with invalid key`);
                            continue;
                        }
                        const document = {
                            _id: row.key,
                            value: parseJsonValue(row.value),
                            createdAt: row.created_at ? new Date(row.created_at) : new Date(),
                            updatedAt: row.updated_at ? new Date(row.updated_at) : new Date(),
                        };
                        if (row.expires_at) document.expiresAt = new Date(row.expires_at);
                        const write = await kv.updateOne(
                            { _id: row.key },
                            { $setOnInsert: document },
                            { upsert: true },
                        );
                        if (write.upsertedCount) kvInserted += 1;
                    }
                }
            }

            report.tables[table] = {
                sourceRows: rows.length,
                archiveInserted: APPLY ? inserted : 0,
                archiveAlreadyPresent: APPLY ? alreadyPresent : 0,
                keyValueInserted: kvInserted,
            };
            report.archivedRows += rows.length;
            report.keyValueRows += kvInserted;
            report.inserted += inserted;
            report.alreadyPresent += alreadyPresent;
        }

        report.note = 'All source rows are retained in postgres_legacy_* archive collections. Only temp_data/cache_data are projected into key_value_store. Structured tables remain archived pending an explicitly reviewed application-key mapping.';
        console.log(JSON.stringify(report, null, 2));
        if (!APPLY) console.log('Dry run only. Re-run with --apply to write; PostgreSQL remains untouched.');
    } finally {
        await Promise.allSettled([source.end(), mongo.close()]);
    }
}

main().catch(error => {
    console.error('PostgreSQL import failed:', error.message);
    process.exitCode = 1;
});
