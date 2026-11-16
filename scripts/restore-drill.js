import { spawnSync } from 'node:child_process';
import { mkdtemp, readdir, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { MongoClient } from 'mongodb';
import dotenv from 'dotenv';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const projectRoot = path.join(__dirname, '..');
dotenv.config({ path: path.join(projectRoot, '.env') });

function databaseNameFromUri(uri) {
    try {
        return new URL(uri).pathname.split('/').filter(Boolean)[0] || 'titanbot';
    } catch {
        return 'titanbot';
    }
}

function runNodeScript(scriptName, args, envOverrides = {}) {
    const scriptPath = path.join(__dirname, scriptName);
    const result = spawnSync(process.execPath, [scriptPath, ...args], {
        cwd: projectRoot,
        encoding: 'utf8',
        env: { ...process.env, ...envOverrides },
        stdio: 'pipe',
    });

    if (result.status !== 0) {
        throw new Error(
            `${scriptName} failed: ${result.stderr || result.stdout || 'Unknown error'}`,
        );
    }
    return result.stdout;
}

async function collectionCounts(database) {
    const names = (await database.listCollections({}, { nameOnly: true }).toArray())
        .map(item => item.name)
        .filter(name => !name.startsWith('system.'));
    const counts = new Map();
    for (const name of names) {
        counts.set(name, await database.collection(name).countDocuments({}));
    }
    return counts;
}

async function run() {
    const uri = process.env.MONGODB_URI;
    if (!uri) throw new Error('Missing MONGODB_URI.');
    const sourceName = process.env.MONGODB_DB_NAME || databaseNameFromUri(uri);
    const stamp = new Date().toISOString().replace(/[^0-9]/g, '').slice(0, 14);
    const drillName = `${sourceName.slice(0, 34)}_restore_drill_${stamp}`;
    const tempDir = await mkdtemp(path.join(os.tmpdir(), 'krew-bot-mongodb-drill-'));
    const client = new MongoClient(uri, {
        serverSelectionTimeoutMS: 10000,
        appName: 'Krew Bot Restore Drill',
    });

    await client.connect();
    try {
        const source = client.db(sourceName);
        const sourceCounts = await collectionCounts(source);
        if (sourceCounts.size === 0) {
            throw new Error('Source database has no collections. Run npm run migrate first.');
        }

        runNodeScript('backup.js', [], { BACKUP_DIR: tempDir });
        const backups = (await readdir(tempDir)).filter(name => name.endsWith('.json.gz'));
        if (backups.length !== 1) {
            throw new Error(`Expected one backup file in the temporary directory, found ${backups.length}.`);
        }

        const backupPath = path.join(tempDir, backups[0]);
        runNodeScript(
            'restore.js',
            ['--file', backupPath, '--database', drillName, '--confirm'],
            { MONGODB_RESTORE_URI: uri },
        );

        const drill = client.db(drillName);
        const restoredCounts = await collectionCounts(drill);
        for (const [name, count] of sourceCounts) {
            if (restoredCounts.get(name) !== count) {
                throw new Error(
                    `Restore verification failed for ${name}: expected ${count} documents, restored ${restoredCounts.get(name) ?? 0}.`,
                );
            }
        }

        const schema = await drill.collection('schema_migrations').findOne({ _id: 'schema_version' });
        if (!schema) throw new Error('Restore verification failed: schema version record is missing.');

        console.log(JSON.stringify({
            success: true,
            sourceDatabase: sourceName,
            drillDatabase: drillName,
            collectionsVerified: sourceCounts.size,
            documentsVerified: [...sourceCounts.values()].reduce((sum, count) => sum + count, 0),
            schemaVersion: schema.version,
        }, null, 2));
    } finally {
        try {
            await client.db(drillName).dropDatabase();
        } finally {
            await client.close();
            await rm(tempDir, { recursive: true, force: true });
        }
    }
}

run().catch(error => {
    console.error('MongoDB restore drill failed:', error.message);
    process.exitCode = 1;
});
