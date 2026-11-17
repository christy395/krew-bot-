import { Pool } from 'pg';
import { logger } from './logger.js';
import { canonicalizeKey, getLegacyVariantsForCanonical } from './database/keys.js';
import { EXPECTED_SCHEMA_LABEL, EXPECTED_SCHEMA_VERSION } from '../config/database/schemaVersion.js';

function resolvePostgresConfig() {
    const connectionString = (
        process.env.DATABASE_URL ||
        process.env.NEON_DATABASE_URL ||
        process.env.POSTGRES_URL ||
        ''
    ).trim();

    return {
        connectionString,
        max: Math.max(1, Number.parseInt(process.env.PG_POOL_MAX || '10', 10) || 10),
        connectionTimeoutMillis: Math.max(1000, Number.parseInt(process.env.PG_CONNECT_TIMEOUT_MS || '10000', 10) || 10000),
        idleTimeoutMillis: Math.max(1000, Number.parseInt(process.env.PG_IDLE_TIMEOUT_MS || '30000', 10) || 30000),
        retries: Math.max(0, Number.parseInt(process.env.PG_RETRIES || '3', 10) || 3),
        backoffBase: Math.max(50, Number.parseInt(process.env.PG_BACKOFF_BASE_MS || '500', 10) || 500),
    };
}

class PostgresDatabase {
    constructor() {
        this.pool = null;
        this.connected = false;
        this.connectionPromise = null;
        this.lastFailureReason = null;
        this.lastFailureMessage = null;
        this.databaseName = null;
    }

    async connect() {
        if (this.isAvailable()) return true;
        if (this.connectionPromise) return this.connectionPromise;
        this.connectionPromise = this.establishConnection();
        try {
            return await this.connectionPromise;
        } finally {
            if (!this.connected) this.connectionPromise = null;
        }
    }

    async establishConnection() {
        const config = resolvePostgresConfig();
        if (!config.connectionString) {
            this.lastFailureReason = 'DATABASE_URL_MISSING';
            this.lastFailureMessage = 'Set DATABASE_URL to your Neon PostgreSQL connection string.';
            logger.error(this.lastFailureMessage);
            return false;
        }

        for (let attempt = 1; attempt <= config.retries + 1; attempt += 1) {
            let pool;
            try {
                pool = new Pool({
                    connectionString: config.connectionString,
                    max: config.max,
                    connectionTimeoutMillis: config.connectionTimeoutMillis,
                    idleTimeoutMillis: config.idleTimeoutMillis,
                    // Neon requires TLS. The connection string should include sslmode=require.
                });
                pool.on('error', error => logger.error('Unexpected PostgreSQL pool error:', error));
                await pool.query('SELECT 1');

                await pool.query(`
                    CREATE TABLE IF NOT EXISTS krew_kv_store (
                        key TEXT PRIMARY KEY,
                        value JSONB NOT NULL,
                        expires_at TIMESTAMPTZ NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                `);
                await pool.query('CREATE INDEX IF NOT EXISTS krew_kv_expires_at_idx ON krew_kv_store (expires_at)');
                await pool.query(`
                    CREATE TABLE IF NOT EXISTS krew_schema_migrations (
                        id TEXT PRIMARY KEY,
                        version INTEGER NOT NULL,
                        label TEXT NOT NULL,
                        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                `);
                await pool.query(`
                    CREATE TABLE IF NOT EXISTS krew_verification_audit (
                        id BIGSERIAL PRIMARY KEY,
                        guild_id TEXT,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        record JSONB NOT NULL
                    )
                `);
                await pool.query('CREATE INDEX IF NOT EXISTS krew_verification_audit_guild_created_idx ON krew_verification_audit (guild_id, created_at DESC)');

                const schema = await pool.query(
                    'SELECT version, label, applied_at FROM krew_schema_migrations WHERE id = $1',
                    ['schema_version'],
                );
                if (schema.rowCount === 0) {
                    await pool.query(
                        'INSERT INTO krew_schema_migrations (id, version, label) VALUES ($1, $2, $3)',
                        ['schema_version', EXPECTED_SCHEMA_VERSION, EXPECTED_SCHEMA_LABEL],
                    );
                } else if (Number(schema.rows[0].version) !== EXPECTED_SCHEMA_VERSION) {
                    const error = new Error(
                        `PostgreSQL schema version mismatch: expected ${EXPECTED_SCHEMA_VERSION}, found ${schema.rows[0].version}`,
                    );
                    error.code = 'SCHEMA_VERSION_MISMATCH';
                    throw error;
                }

                this.pool = pool;
                this.connected = true;
                this.databaseName = this.getDatabaseNameFromUrl(config.connectionString);
                this.lastFailureReason = null;
                this.lastFailureMessage = null;
                logger.info('Neon PostgreSQL connected successfully');

                if (process.env.AUTO_MIGRATE !== 'false') {
                    try {
                        const migration = await this.runKeyMigration();
                        if (migration.migrated || migration.skipped || migration.errors) {
                            logger.info('PostgreSQL startup key migration finished', migration);
                        }
                    } catch (migrationError) {
                        logger.error('PostgreSQL startup key migration failed:', migrationError);
                    }
                }
                return true;
            } catch (error) {
                if (pool) {
                    try { await pool.end(); } catch (closeError) {
                        logger.warn('Could not close failed PostgreSQL pool:', closeError.message);
                    }
                }
                this.lastFailureReason = error.code || 'POSTGRES_CONNECTION_FAILED';
                this.lastFailureMessage = error.message || 'Unknown PostgreSQL error';
                if (error.code === 'SCHEMA_VERSION_MISMATCH' || attempt >= config.retries + 1) {
                    logger.error('Failed to initialize Neon PostgreSQL:', error.message);
                    return false;
                }
                const delay = config.backoffBase * (2 ** (attempt - 1));
                logger.warn(`PostgreSQL connection attempt ${attempt} failed: ${error.message}`);
                await new Promise(resolve => setTimeout(resolve, delay));
            }
        }
        return false;
    }

    getDatabaseNameFromUrl(connectionString) {
        try {
            return new URL(connectionString).pathname.replace(/^\//, '') || 'neon';
        } catch {
            return 'neon';
        }
    }

    isAvailable() {
        return Boolean(this.connected && this.pool);
    }

    getLastFailure() {
        return { reason: this.lastFailureReason, message: this.lastFailureMessage };
    }

    async get(key, defaultValue = null) {
        if (!this.isAvailable()) return defaultValue;
        try {
            const canonicalKey = canonicalizeKey(key);
            const candidates = [...new Set([
                canonicalKey,
                ...(key !== canonicalKey ? [key] : []),
                ...getLegacyVariantsForCanonical(canonicalKey),
            ])].filter(value => typeof value === 'string' && value.length > 0);

            for (const candidate of candidates) {
                const result = await this.pool.query(
                    'SELECT value FROM krew_kv_store WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())',
                    [candidate],
                );
                if (result.rowCount) return result.rows[0].value ?? defaultValue;
            }
            return defaultValue;
        } catch (error) {
            logger.error(`PostgreSQL get failed for key ${key}:`, error);
            return defaultValue;
        }
    }

    async set(key, value, ttl = null) {
        if (!this.isAvailable()) return false;
        if (typeof key !== 'string' || key.length === 0) {
            throw new TypeError('Database keys must be non-empty strings');
        }
        try {
            const canonicalKey = canonicalizeKey(key);
            const ttlSeconds = Number(ttl);
            const expiresAt = Number.isFinite(ttlSeconds) && ttlSeconds > 0
                ? new Date(Date.now() + ttlSeconds * 1000)
                : null;
            await this.pool.query(`
                INSERT INTO krew_kv_store (key, value, expires_at)
                VALUES ($1, $2::jsonb, $3)
                ON CONFLICT (key) DO UPDATE
                SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at, updated_at = NOW()
            `, [canonicalKey, JSON.stringify(value ?? null), expiresAt]);
            return true;
        } catch (error) {
            logger.error(`PostgreSQL set failed for key ${key}:`, error);
            return false;
        }
    }

    async delete(key) {
        if (!this.isAvailable()) return false;
        try {
            const canonicalKey = canonicalizeKey(key);
            const keys = [...new Set([
                canonicalKey,
                key,
                ...getLegacyVariantsForCanonical(canonicalKey),
            ])].filter(value => typeof value === 'string' && value.length > 0);
            await this.pool.query('DELETE FROM krew_kv_store WHERE key = ANY($1::text[])', [keys]);
            return true;
        } catch (error) {
            logger.error(`PostgreSQL delete failed for key ${key}:`, error);
            return false;
        }
    }

    async list(prefix = '') {
        if (!this.isAvailable() || typeof prefix !== 'string') return [];
        try {
            const result = await this.pool.query(
                'SELECT key FROM krew_kv_store WHERE LEFT(key, LENGTH($1)) = $1 AND (expires_at IS NULL OR expires_at > NOW()) ORDER BY key',
                [prefix],
            );
            return result.rows.map(row => row.key);
        } catch (error) {
            logger.error(`PostgreSQL list failed for prefix ${prefix}:`, error);
            return [];
        }
    }

    async exists(key) {
        if (!this.isAvailable()) return false;
        try {
            const canonicalKey = canonicalizeKey(key);
            const candidates = [...new Set([
                canonicalKey,
                key,
                ...getLegacyVariantsForCanonical(canonicalKey),
            ])];
            const result = await this.pool.query(
                'SELECT 1 FROM krew_kv_store WHERE key = ANY($1::text[]) AND (expires_at IS NULL OR expires_at > NOW()) LIMIT 1',
                [candidates],
            );
            return result.rowCount > 0;
        } catch (error) {
            logger.error(`PostgreSQL exists failed for key ${key}:`, error);
            return false;
        }
    }

    async increment(key, amount = 1) {
        if (!this.isAvailable()) return amount;
        const canonicalKey = canonicalizeKey(key);
        try {
            const result = await this.pool.query(`
                INSERT INTO krew_kv_store (key, value)
                VALUES ($1, to_jsonb($2::numeric))
                ON CONFLICT (key) DO UPDATE
                SET value = to_jsonb(COALESCE((krew_kv_store.value #>> '{}')::numeric, 0) + $2::numeric),
                    expires_at = NULL,
                    updated_at = NOW()
                RETURNING value
            `, [canonicalKey, amount]);
            return Number(result.rows[0].value);
        } catch (error) {
            logger.error(`PostgreSQL increment failed for key ${key}:`, error);
            return amount;
        }
    }

    async decrement(key, amount = 1) {
        return this.increment(key, -amount);
    }

    async insertVerificationAudit(record) {
        if (!this.isAvailable()) return false;
        try {
            const createdAt = record.createdAt ? new Date(record.createdAt) : new Date();
            await this.pool.query(
                'INSERT INTO krew_verification_audit (guild_id, created_at, record) VALUES ($1, $2, $3::jsonb)',
                [record.guildId ? String(record.guildId) : null, createdAt, JSON.stringify(record)],
            );
            return true;
        } catch (error) {
            logger.error('PostgreSQL verification audit insert failed:', error);
            return false;
        }
    }

    async getSchemaStatus() {
        if (!this.isAvailable()) return null;
        const result = await this.pool.query(
            'SELECT version, label, applied_at FROM krew_schema_migrations WHERE id = $1',
            ['schema_version'],
        );
        if (!result.rowCount) return null;
        return {
            version: Number(result.rows[0].version),
            label: result.rows[0].label,
            appliedAt: result.rows[0].applied_at,
        };
    }

    async runKeyMigration({ dryRun = false, force = false, logger: migrationLogger = logger } = {}) {
        if (!this.isAvailable()) throw new Error('PostgreSQL is not connected');
        const markerKey = '__meta:key_migration:v1';
        const summary = { migrated: 0, skipped: 0, errors: 0 };
        if (!dryRun && !force && await this.exists(markerKey)) {
            return { ...summary, alreadyDone: true };
        }

        const result = await this.pool.query('SELECT key, value, expires_at FROM krew_kv_store ORDER BY key');
        for (const row of result.rows) {
            const legacyKey = row.key;
            if (legacyKey === markerKey) continue;
            const canonicalKey = canonicalizeKey(legacyKey);
            if (legacyKey === canonicalKey) continue;
            try {
                const existing = await this.pool.query('SELECT 1 FROM krew_kv_store WHERE key = $1', [canonicalKey]);
                if (existing.rowCount) {
                    summary.skipped += 1;
                    if (!dryRun) await this.pool.query('DELETE FROM krew_kv_store WHERE key = $1', [legacyKey]);
                    continue;
                }
                summary.migrated += 1;
                migrationLogger.info(`Migrate key ${legacyKey} -> ${canonicalKey}`);
                if (!dryRun) {
                    await this.pool.query(
                        'INSERT INTO krew_kv_store (key, value, expires_at) VALUES ($1, $2::jsonb, $3) ON CONFLICT (key) DO NOTHING',
                        [canonicalKey, JSON.stringify(row.value), row.expires_at],
                    );
                    await this.pool.query('DELETE FROM krew_kv_store WHERE key = $1', [legacyKey]);
                }
            } catch (error) {
                summary.errors += 1;
                migrationLogger.error(`Failed to migrate key ${legacyKey}:`, error);
            }
        }

        if (!dryRun && summary.errors === 0) {
            await this.set(markerKey, { completedAt: new Date().toISOString(), ...summary });
        }
        migrationLogger.info('PostgreSQL key migration completed', summary);
        return summary;
    }

    async getInfo() {
        if (!this.isAvailable()) return null;
        try {
            const result = await this.pool.query('SELECT current_database() AS database_name');
            return { connected: true, databaseName: result.rows[0].database_name, tableName: 'krew_kv_store' };
        } catch (error) {
            logger.error('Could not read PostgreSQL status:', error);
            return null;
        }
    }

    async disconnect() {
        if (this.pool) await this.pool.end();
        this.pool = null;
        this.connected = false;
        this.connectionPromise = null;
        logger.info('Neon PostgreSQL connection closed');
    }
}

const postgresDb = new PostgresDatabase();
export { PostgresDatabase, postgresDb };
