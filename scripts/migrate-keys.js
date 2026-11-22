import dotenv from 'dotenv';

dotenv.config();

// Prevent connect() from applying the startup migration before a dry-run or
// explicitly requested migration can execute.
process.env.AUTO_MIGRATE = 'false';
process.env.MONGODB_ALLOW_SCHEMA_INIT = 'false';

const { mongoDb } = await import('../src/utils/mongoDatabase.js');
const dryRun = process.argv.includes('--dry-run');
const force = process.argv.includes('--force');

async function run() {
    const connected = await mongoDb.connect();
    if (!connected) {
        const failure = mongoDb.getLastFailure?.();
        throw new Error(failure?.message || 'Could not connect to MongoDB. Check MONGODB_URI.');
    }

    try {
        const result = await mongoDb.runKeyMigration({ dryRun, force });
        console.log(JSON.stringify({ dryRun, force, ...result }, null, 2));
        if (result.errors > 0) process.exitCode = 1;
    } finally {
        await mongoDb.disconnect();
    }
}

run().catch(error => {
    console.error('MongoDB key migration failed:', error.message);
    process.exitCode = 1;
});
