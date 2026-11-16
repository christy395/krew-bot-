import dotenv from 'dotenv';
import { readFile } from 'node:fs/promises';
import { gunzipSync } from 'node:zlib';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { EJSON, MongoClient } from 'mongodb';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.join(__dirname, '..', '.env') });

function parseArgs(argv) {
    const args = {};
    for (let i = 0; i < argv.length; i += 1) {
        const token = argv[i];
        if (!token.startsWith('--')) continue;
        const [key, inline] = token.slice(2).split('=');
        if (inline !== undefined) args[key] = inline;
        else if (argv[i + 1] && !argv[i + 1].startsWith('--')) args[key] = argv[++i];
        else args[key] = true;
    }
    return args;
}

async function run() {
    const args = parseArgs(process.argv.slice(2));
    const backupPath = args.file || args.backup;
    if (!backupPath) throw new Error('Provide a backup file with --file <path>.');
    if (args.confirm !== true && args.confirm !== 'true') {
        throw new Error('Restore replaces matching collections. Re-run with --confirm after verifying the target database.');
    }

    const uri = process.env.MONGODB_RESTORE_URI || process.env.MONGODB_URI;
    if (!uri) throw new Error('Missing MONGODB_RESTORE_URI or MONGODB_URI.');
    const content = gunzipSync(await readFile(path.resolve(backupPath))).toString('utf8');
    const snapshot = EJSON.parse(content);
    if (snapshot?.format !== 'titanbot-mongodb-backup-v1' || !Array.isArray(snapshot.collections)) {
        throw new Error('The selected file is not a supported TitanBot MongoDB backup.');
    }

    const databaseName = args.database || process.env.MONGODB_DB_NAME || snapshot.databaseName;
    if (!databaseName) throw new Error('Could not determine the restore database name.');

    const client = new MongoClient(uri, { serverSelectionTimeoutMS: 10000, appName: 'TitanBot Restore' });
    await client.connect();
    try {
        const database = client.db(databaseName);
        for (const entry of snapshot.collections) {
            if (!entry?.name || !Array.isArray(entry.documents)) continue;
            if (entry.name.startsWith('system.')) continue;
            const collection = database.collection(entry.name);
            await collection.deleteMany({});
            for (let index = 0; index < entry.documents.length; index += 500) {
                const batch = entry.documents.slice(index, index + 500);
                if (batch.length) await collection.insertMany(batch, { ordered: false });
            }
            console.log(`Restored ${entry.documents.length} documents into ${entry.name}`);
        }

        await database.collection('key_value_store').createIndex(
            { expiresAt: 1 },
            { expireAfterSeconds: 0, sparse: true, name: 'ttl_expires_at' },
        );
        await database.collection('verification_audit').createIndex(
            { guildId: 1, createdAt: -1 },
            { name: 'guild_audit_created_at' },
        );
        console.log(`Restore completed in database: ${databaseName}`);
    } finally {
        await client.close();
    }
}

run().catch(error => {
    console.error('MongoDB restore failed:', error.message);
    process.exitCode = 1;
});
