import { readFile } from 'node:fs/promises';
import pg from 'pg';

// Recreates a disposable test database from sql/schema.sql (the dev database is never touched).
// Runs in the main process (test.env is not applied here), so it derives the same URLs itself.
const host = process.env.TEST_PG_HOST ?? 'postgres:postgres@localhost:5432';

export default async function setup() {
  const admin = new pg.Client({ connectionString: `postgres://${host}/postgres` });
  await admin.connect();
  await admin.query('DROP DATABASE IF EXISTS encounters_test WITH (FORCE)');
  await admin.query('CREATE DATABASE encounters_test');
  await admin.end();

  const db = new pg.Client({ connectionString: `postgres://${host}/encounters_test` });
  await db.connect();
  await db.query(await readFile(new URL('../sql/schema.sql', import.meta.url), 'utf8'));
  await db.end();
}
