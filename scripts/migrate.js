import dotenv from 'dotenv';

dotenv.config();

// Keep status/check/apply commands from silently migrating legacy keys.
// Use `npm run migrate:keys` (or its dry-run variant) for that operation.
process.env.AUTO_MIGRATE = 'false';

const { mongoDb } = await import('../src/utils/mongoDatabase.js');

const command = process.argv[2] || 'status';

async function run() {
    const connected = await mongoDb.connect();
    if (!connected) {
        const failure = mongoDb.getLastFailure?.();
        throw new Error(failure?.message || 'Could not connect to MongoDB. Check MONGODB_URI.');
    }

    try {
        const schema = await mongoDb.getSchemaStatus();
        if (command === 'check') {
            if (!schema) throw new Error('MongoDB schema version record is missing. Run npm run migrate.');
            console.log(`MongoDB schema OK: version ${schema.version} (${schema.label})`);
        } else if (command === 'status') {
            console.log(JSON.stringify({
                database: mongoDb.databaseName,
                connected: mongoDb.isAvailable(),
                schema,
            }, null, 2));
        } else if (command === 'apply') {
            console.log(`MongoDB indexes and schema ledger are ready for ${mongoDb.databaseName}.`);
            console.log(JSON.stringify(schema, null, 2));
        } else {
            throw new Error(`Unknown migration command "${command}". Use apply, check, or status.`);
        }
    } finally {
        await mongoDb.disconnect();
    }
}

run().catch(error => {
    console.error('MongoDB migration command failed:', error.message);
    process.exitCode = 1;
});
