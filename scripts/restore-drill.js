import dotenv from 'dotenv';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { MongoClient } from 'mongodb';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.join(__dirname, '..', '.env') });

function databaseNameFromUri(uri) {
    try {
        return new URL(uri).pathname.replace(/^\\//, '').split('/')[0] || 'titanbot';
    } catch {
        return 'titanbot';
    }
}

async function copyCollection(source, target, name) {
    const sourceCollection = source.collection(name);
    const targetCollection = target.collection(name);
    let copied = 0;
    const batch = [];

    for await (const document of sourceCollection.find({})) {
        batch.push(document);
        if (batch.length >= 500) {
            await targetCollection.insertMany(batch.splice(0, batch.length), { ordered: false });
            copied += 500;
        }
    }
    if (batch.length) {
        const count = batch.length;
        await targetCollection.insertMany(batch, { ordered: false });
        copied += count;
    }

    const sourceCount = await sourceCollection.countDocuments({});
    const targetCount = await targetCollection.countDocuments({});
    if (sourceCount !== targetCount || sourceCount !== copied) {
        throw new Error(`Restore drill count mismatch for ${name}: source=${sourceCount}, copied=${copied}, target=${targetCount}`);
    }
    return sourceCount;
}

async function run() {
    const uri = process.env.MONGODB_URI;
    if (!uri) throw new Error('Missing MONGODB_URI.');
    const sourceName = process.env.MONGODB_DB_NAME || databaseNameFromUri(uri);
    const stamp = new Date().toISOString().replace(/[^0-9]/g, '').slice(0, 14);
    const drillName = `${sourceName.slice(0, 42)}_restore_drill_${stamp}`;

    const client = new MongoClient(uri, { serverSelectionTimeoutMS: 10000, appName: 'TitanBot Restore Drill' });
    await client.connect();
    try {
        const source = client.db(sourceName);
        const drill = client.db(drillName);
        const names = (await source.listCollections({}, { nameOnly: true }).toArray())
            .map(item => item.name)
            .filter(name => !name.startsWith('system.'));
        if (names.length === 0) throw new Error('Source database has no collections. Run npm run migrate first.');

        let copiedCollections = 0;
        let copiedDocuments = 0;
        for (const name of names) {
            copiedDocuments += await copyCollection(source, drill, name);
            copiedCollections += 1;
        }

        const schema = await drill.collection('schema_migrations').findOne({ _id: 'schema_version' });
        if (!schema) throw new Error('Restore drill did not restore the schema version record.');

        console.log(JSON.stringify({
            success: true,
            sourceDatabase: sourceName,
            drillDatabase: drillName,
            copiedCollections,
            copiedDocuments,
            schemaVersion: schema.version,
        }, null, 2));
    } finally {
        await client.db(drillName).dropDatabase();
        await client.close();
    }
}

run().catch(error => {
    console.error('MongoDB restore drill failed:', error.message);
    process.exitCode = 1;
});
