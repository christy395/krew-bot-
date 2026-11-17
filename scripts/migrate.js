import dotenv from 'dotenv';

dotenv.config();
process.env.AUTO_MIGRATE = 'false';

const { postgresDb } = await import('../src/utils/postgresDatabase.js');
const command = process.argv[2] || 'status';

async function run() {
    const connected = await postgresDb.connect();
    if (!connected) {
        const failure = postgresDb.getLastFailure?.();
        throw new Error(failure?.message || 'Could not connect to Neon. Check DATABASE_URL.');
    }

    try {
        const schema = await postgresDb.getSchemaStatus();
        if (command === 'check') {
            if (!schema) throw new Error('PostgreSQL schema version record is missing. Run npm run migrate.');
            console.log(`PostgreSQL schema OK: version ${schema.version} (${schema.label})`);
        } else if (command === 'status') {
            console.log(JSON.stringify({
                database: schema ? 'connected' : postgresDb.databaseName,
                connected: postgresDb.isAvailable(),
                schema,
            }, null, 2));
        } else if (command === 'apply') {
            console.log('Neon PostgreSQL tables and schema ledger are ready.');
            console.log(JSON.stringify(schema, null, 2));
        } else {
            throw new Error(`Unknown migration command "${command}". Use apply, check, or status.`);
        }
    } finally {
        await postgresDb.disconnect();
    }
}

run().catch(error => {
    console.error('PostgreSQL migration command failed:', error.message);
    process.exitCode = 1;
});
