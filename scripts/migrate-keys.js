import dotenv from 'dotenv';

dotenv.config();
process.env.AUTO_MIGRATE = 'false';

const { postgresDb } = await import('../src/utils/postgresDatabase.js');
const dryRun = process.argv.includes('--dry-run');
const force = process.argv.includes('--force');

async function run() {
    const connected = await postgresDb.connect();
    if (!connected) {
        const failure = postgresDb.getLastFailure?.();
        throw new Error(failure?.message || 'Could not connect to Neon. Check DATABASE_URL.');
    }

    try {
        const result = await postgresDb.runKeyMigration({ dryRun, force });
        console.log(JSON.stringify({ dryRun, force, ...result }, null, 2));
        if (result.errors > 0) process.exitCode = 1;
    } finally {
        await postgresDb.disconnect();
    }
}

run().catch(error => {
    console.error('PostgreSQL key migration failed:', error.message);
    process.exitCode = 1;
});
