import createConnectionPool, { sql } from "../";
import { test } from "node:test";
import assert from "node:assert";

async function testPool(t) {
  const db = createConnectionPool();
  t.after(db.dispose.bind(db));
  return db;
}

test("streaming", async (t) => {
  const db = await testPool(t);
  await db.query(
    sql`CREATE TABLE stream_values (id BIGINT NOT NULL PRIMARY KEY);`,
  );
  const allValues = [];
  for (let batch = 0; batch < 10; batch++) {
    const batchValues = [];
    for (let i = 0; i < 10; i++) {
      const value = batch * 10 + i;
      batchValues.push(value);
      allValues.push(value);
    }
    await db.query(sql`
      INSERT INTO stream_values (id)
      VALUES ${sql.join(
        batchValues.map((v) => sql`(${v})`),
        sql`,`,
      )};
    `);
  }
  const results = [];
  for await (const row of db.queryStream(sql`SELECT * FROM stream_values`)) {
    results.push(row.id);
  }
  assert.deepStrictEqual(results, allValues);
});
