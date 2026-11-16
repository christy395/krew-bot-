import { MongoClient } from 'mongodb';
import { logger } from './logger.js';
import { canonicalizeKey, getLegacyVariantsForCanonical } from './database/keys.js';
import { EXPECTED_SCHEMA_LABEL, EXPECTED_SCHEMA_VERSION } from '../config/database/schemaVersion.js';

function resolveMongoConfig() {
    const uri = (process.env.MONGODB_URI || 'mongodb://127.0.0.1:27017/titanbot').trim();
    let databaseName = (process.env.MONGODB_DB_NAME || '').trim();

    if (!databaseName) {
        try {
            databaseName = new URL(uri).pathname.replace(/^\\//, '').split('/')[0];
        } catch {
            databaseName = '';
        }
    }

    return {
        uri,
        databaseName: databaseName || 'titanbot',
        options: {
            maxPoolSize: Math.max(1, Number.parseInt(process.env.MONGODB_MAX_POOL_SIZE || '20', 10) || 20),
            minPoolSize: Math.max(0, Number.parseInt(process.env.MONGODB_MIN_POOL_SIZE || '0', 10) || 0),
            connectTimeoutMS: Math.max(1000, Number.parseInt(process.env.MONGODB_CONNECT_TIMEOUT_MS || '10000', 10) || 10000),
            serverSelectionTimeoutMS: Math.max(1000, Number.parseInt(process.env.MONGODB_SERVER_SELECTION_TIMEOUT_MS || '10000', 10) || 10000),
            retryWrites: true,
            appName: 'TitanBot',
        },
        retries: Math.max(0, Number.parseInt(process.env.MONGODB_RETRIES || '3', 10) || 0),
        backoffBase: Math.max(50, Number.parseInt(process.env.MONGODB_BACKOFF_BASE_MS || '500', 10) || 500),
    };
}

function escapeRegex(value) {
    return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

class MongoDatabase {
    constructor() {
        this.client = null;
        this.database = null;
        this.collection = null;
        this.isConnected = false;
        this.connectionPromise = null;
        this.lastFailureReason = null;
        this.lastFailureMessage = null;
        this.databaseName = null;
    }

    async connect() {
        if (this.isAvailable()) return true;
        if (this.connectionPromise) return this.connectionPromise;
        this.connectionPromise = this._establishConnection();
        try {
            return await this.connectionPromise;
        } finally {
            if (!this.isConnected) this.connectionPromise = null;
        }
    }

    async _establishConnection() {
        const config = resolveMongoConfig();
        for (let attempt = 1; attempt <= config.retries + 1; attempt += 1) {
            try {
                const client = new MongoClient(config.uri, config.options);
                await client.connect();
                this.client = client;
                const database = client.db(config.databaseName);
                this.database = database;
                this.databaseName = config.databaseName;
                await database.command({ ping: 1 });

                const collection = database.collection('key_value_store');
                await collection.createIndex(
                    { expiresAt: 1 },
                    { expireAfterSeconds: 0, sparse: true, name: 'ttl_expires_at' },
                );
                await collection.createIndex({ createdAt: 1 }, { name: 'created_at' });
                const audit = database.collection('verification_audit');
                await audit.createIndex({ guildId: 1, createdAt: -1 }, { name: 'guild_audit_created_at' });
                const migrations = database.collection('schema_migrations');

                this.collection = collection;

                const schema = await migrations.findOne({ _id: 'schema_version' });
                if (!schema) {
                    await migrations.insertOne({
                        _id: 'schema_version',
                        version: EXPECTED_SCHEMA_VERSION,
                        label: EXPECTED_SCHEMA_LABEL,
                        appliedAt: new Date(),
                    });
                } else if (Number(schema.version) !== EXPECTED_SCHEMA_VERSION) {
                    const error = new Error(
                        `MongoDB schema version mismatch: expected ${EXPECTED_SCHEMA_VERSION}, found ${schema.version}`,
                    );
                    error.code = 'SCHEMA_VERSION_MISMATCH';
                    throw error;
                }

                this.isConnected = true;
                this.lastFailureReason = null;
                this.lastFailureMessage = null;
                logger.info(`MongoDB connected successfully (database: ${config.databaseName})`);

                if (process.env.AUTO_MIGRATE !== 'false') {
                    try {
                        const migration = await this.runKeyMigration();
                        if (migration.migrated || migration.skipped || migration.errors) {
                            logger.info('MongoDB startup key migration finished', migration);
                        }
                    } catch (migrationError) {
                        logger.error('MongoDB startup key migration failed (continuing with legacy reads):', migrationError);
                    }
                }
                return true;
            } catch (error) {
                this.lastFailureReason = error.code || 'MONGODB_CONNECTION_FAILED';
                this.lastFailureMessage = error.message || 'Unknown MongoDB error';
                if (this.client) {
                    try { await this.client.close(); } catch (closeError) {
                        logger.warn('Could not close failed MongoDB connection:', closeError.message);
                    }
                }
                this.client = null;
                this.database = null;
                this.collection = null;
                this.isConnected = false;

                if (error.code === 'SCHEMA_VERSION_MISMATCH' || attempt >= config.retries + 1) {
                    logger.error('Failed to initialize MongoDB:', error.message);
                    return false;
                }

                const delay = config.backoffBase * (2 ** (attempt - 1));
                logger.warn(`MongoDB connection attempt ${attempt} failed: ${error.message}`);
                await new Promise(resolve => setTimeout(resolve, delay));
            }
        }
        return false;
    }

    isAvailable() {
        return Boolean(this.isConnected && this.database && this.collection);
    }

    getLastFailure() {
        return { reason: this.lastFailureReason, message: this.lastFailureMessage };
    }

    async _findDocument(key) {
        const now = new Date();
        return this.collection.findOne({
            _id: key,
            $or: [
                { expiresAt: { $exists: false } },
                { expiresAt: null },
                { expiresAt: { $gt: now } },
            ],
        });
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
                const document = await this._findDocument(candidate);
                if (document) return document.value ?? defaultValue;
            }
            return defaultValue;
        } catch (error) {
            logger.error(`MongoDB get failed for key ${key}:`, error);
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
            const now = new Date();
            const update = {
                $set: { value: value ?? null, updatedAt: now },
                $setOnInsert: { createdAt: now },
            };

            if (Number.isFinite(Number(ttl)) && Number(ttl) > 0) {
                update.$set.expiresAt = new Date(Date.now() + Number(ttl) * 1000);
            } else {
                update.$unset = { expiresAt: '' };
            }

            await this.collection.updateOne({ _id: canonicalKey }, update, { upsert: true });
            return true;
        } catch (error) {
            logger.error(`MongoDB set failed for key ${key}:`, error);
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
            await this.collection.deleteMany({ _id: { $in: keys } });
            return true;
        } catch (error) {
            logger.error(`MongoDB delete failed for key ${key}:`, error);
            return false;
        }
    }

    async list(prefix = '') {
        if (!this.isAvailable()) return [];
        if (typeof prefix !== 'string') return [];
        try {
            const now = new Date();
            const documents = await this.collection.find({
                _id: { $regex: `^${escapeRegex(prefix)}` },
                $or: [
                    { expiresAt: { $exists: false } },
                    { expiresAt: null },
                    { expiresAt: { $gt: now } },
                ],
            }, { projection: { _id: 1 } }).sort({ _id: 1 }).toArray();
            return documents.map(document => String(document._id));
        } catch (error) {
            logger.error(`MongoDB list failed for prefix ${prefix}:`, error);
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
            for (const candidate of candidates) {
                if (await this._findDocument(candidate)) return true;
            }
            return false;
        } catch (error) {
            logger.error(`MongoDB exists failed for key ${key}:`, error);
            return false;
        }
    }

    async increment(key, amount = 1) {
        if (!this.isAvailable()) return amount;
        const canonicalKey = canonicalizeKey(key);
        try {
            await this.collection.updateOne(
                { _id: canonicalKey },
                {
                    $inc: { value: amount },
                    $set: { updatedAt: new Date() },
                    $setOnInsert: { createdAt: new Date() },
                    $unset: { expiresAt: '' },
                },
                { upsert: true },
            );
            const document = await this.collection.findOne({ _id: canonicalKey });
            return document?.value ?? amount;
        } catch (error) {
            logger.error(`MongoDB increment failed for key ${key}:`, error);
            return amount;
        }
    }

    async decrement(key, amount = 1) {
        return this.increment(key, -amount);
    }

    async insertVerificationAudit(record) {
        if (!this.isAvailable()) return false;
        try {
            const createdAt = record.createdAt instanceof Date
                ? record.createdAt
                : new Date(record.createdAt || Date.now());
            await this.database.collection('verification_audit').insertOne({
                ...record,
                createdAt,
            });
            return true;
        } catch (error) {
            logger.error('MongoDB verification audit insert failed:', error);
            return false;
        }
    }

    async getSchemaStatus() {
        if (!this.isAvailable()) return null;
        const schema = await this.database.collection('schema_migrations')
            .findOne({ _id: 'schema_version' });
        return schema ? {
            version: Number(schema.version),
            label: schema.label,
            appliedAt: schema.appliedAt,
        } : null;
    }

    async runKeyMigration({ dryRun = false, force = false, logger: migrationLogger = logger } = {}) {
        if (!this.isAvailable()) throw new Error('MongoDB is not connected');
        const markerId = '__meta:key_migration:v1';
        const summary = { migrated: 0, skipped: 0, errors: 0 };

        if (!dryRun && !force && await this.collection.findOne({ _id: markerId })) {
            return { ...summary, alreadyDone: true };
        }

        const cursor = this.collection.find({ $and: [{ _id: { $type: 'string' } }, { _id: { $ne: markerId } }] });

        for await (const document of cursor) {
            const legacyKey = document._id;
            const canonicalKey = canonicalizeKey(legacyKey);
            if (legacyKey === canonicalKey) continue;

            try {
                const existing = await this.collection.findOne({ _id: canonicalKey });
                if (existing) {
                    summary.skipped += 1;
                    if (!dryRun) await this.collection.deleteOne({ _id: legacyKey });
                    continue;
                }

                summary.migrated += 1;
                migrationLogger.info(`Migrate key ${legacyKey} -> ${canonicalKey}`);
                if (!dryRun) {
                    const replacement = { ...document, _id: canonicalKey, updatedAt: new Date() };
                    delete replacement._id;
                    await this.collection.updateOne(
                        { _id: canonicalKey },
                        { $setOnInsert: replacement },
                        { upsert: true },
                    );
                    await this.collection.deleteOne({ _id: legacyKey });
                }
            } catch (error) {
                summary.errors += 1;
                migrationLogger.error(`Failed to migrate key ${legacyKey}:`, error);
            }
        }

        if (!dryRun && summary.errors === 0) {
            await this.collection.updateOne(
                { _id: markerId },
                {
                    $set: {
                        value: { completedAt: new Date(), ...summary },
                        updatedAt: new Date(),
                    },
                    $setOnInsert: { createdAt: new Date() },
                },
                { upsert: true },
            );
        }

        migrationLogger.info('MongoDB key migration completed', summary);
        return summary;
    }

    async getInfo() {
        if (!this.isAvailable()) return null;
        try {
            await this.database.command({ ping: 1 });
            return { connected: true, databaseName: this.databaseName, collectionName: 'key_value_store' };
        } catch (error) {
            logger.error('Could not read MongoDB status:', error);
            return null;
        }
    }

    async disconnect() {
        if (this.client) {
            await this.client.close();
        }
        this.client = null;
        this.database = null;
        this.collection = null;
        this.isConnected = false;
        this.connectionPromise = null;
        logger.info('MongoDB connection closed');
    }
}

const mongoDb = new MongoDatabase();
export { MongoDatabase, mongoDb };
