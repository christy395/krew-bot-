import dotenv from 'dotenv';
import { createGzip } from 'node:zlib';
import { createWriteStream } from 'node:fs';
import { mkdir, readdir, stat, unlink } from 'node:fs/promises';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { EJSON, MongoClient } from 'mongodb';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.join(__dirname, '..', '.env') });

function timestamp() {
    return new Date().toISOString().replace(/[-:]/g, '').replace(/\.\d{3}Z$/, 'Z');
}

function databaseNameFromUri(uri) {
    try {
        return new URL(uri).pathname.replace(/^\\//, '').split('/')[0] || 'titanbot';
    } catch {
        return 'titanbot';
    }
}

async function pruneBackups(directory, retentionDays) {
    if (!Number.isFinite(retentionDays) || retentionDays <= 0) return;
    const cutoff = Date.now() - retentionDays * 24 * 60 * 60 * 1000;
    for (const name of await readdir(directory)) {
        if (!name.endsWith('.json.gz')) continue;
        const fullPath = path.join(directory, name);
        if ((await stat(fullPath)).mtimeMs < cutoff) await unlink(fullPath);
    }
}

async function run() {
    const uri = process.env.MONGODB_URI;
    if (!uri) throw new Error('Missing MONGODB_URI.');
    const databaseName = process.env.MONGODB_DB_NAME || databaseNameFromUri(uri);
    const backupDir = path.resolve(process.env.BACKUP_DIR || path.join(process.cwd(), 'backups'));
    const retentionDays = Number.parseInt(process.env.BACKUP_RETENTION_DAYS || '14', 10);
    await mkdir(backupDir, { recursive: true });

    const client = new MongoClient(uri, { serverSelectionTimeoutMS: 10000, appName: 'TitanBot Backup' });
    await client.connect();
    try {
        const database = client.db(databaseName);
        const collectionNames = (await database.listCollections({}, { nameOnly: true }).toArray())
            .map(item => item.name)
            .filter(name => !name.startsWith('system.'));
        const snapshot = {
            format: 'titanbot-mongodb-backup-v1',
            databaseName,
            createdAt: new Date(),
            collections: [],
        };

        for (const name of collectionNames) {
            const documents = await database.collection(name).find({}).toArray();
            snapshot.collections.push({ name, documents });
        }

        const backupPath = path.join(backupDir, `mongodb-backup-${timestamp()}.json.gz`);
        const content = EJSON.stringify(snapshot, { relaxed: false });
        await pipeline(Readable.from([content]), createGzip({ level: 9 }), createWriteStream(backupPath));
        await pruneBackups(backupDir, retentionDays);
        console.log(`MongoDB backup created: ${backupPath}`);
        console.log(`Collections saved: ${snapshot.collections.length}`);
    } finally {
        await client.close();
    }
}

run().catch(error => {
    console.error('MongoDB backup failed:', error.message);
    process.exitCode = 1;
});
