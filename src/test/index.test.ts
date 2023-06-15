import createConnectionPool, { sql } from "../";
import { test } from "node:test";
import assert from "node:assert";

test("error messages", async (t) => {
  const db = createConnectionPool();
  t.after(db.dispose.bind(db));
  await assert.rejects(db.query(sql`SELECT * FRM 'baz;`), "SQLITE_ERROR");
});

test("query", async (t) => {
  const db = createConnectionPool();
  t.after(db.dispose.bind(db));
  const [{ foo }] = await db.query(sql`SELECT 1 + 1 as foo`);
  assert.strictEqual(foo, 2);
});

test("query with params", async (t) => {
  const db = createConnectionPool();
  t.after(db.dispose.bind(db));
  const [{ foo }] = await db.query(
    sql`SELECT 1 + ${41} as ${sql.ident("foo")}`
  );
  assert.strictEqual(foo, 42);
});

test("bigint", async (t) => {
  const db = createConnectionPool();
  t.after(db.dispose.bind(db));
  await db.query(
    sql`CREATE TABLE bigint_test_bigints (id BIGINT NOT NULL PRIMARY KEY);`
  );
  await db.query(sql`
    INSERT INTO bigint_test_bigints (id)
    VALUES (1),
           (2),
           (42);
  `);
  const result = await db.query(sql`SELECT id from bigint_test_bigints;`);
  assert.deepStrictEqual(result, [{ id: 1 }, { id: 2 }, { id: 42 }]);
});

test("transaction", async (t) => {
  const db = createConnectionPool();
  t.after(db.dispose.bind(db));
  const result = await db.tx(async (tx) => {
    const a = await tx.query(sql`SELECT 1 + ${41} as ${sql.ident("foo")}`);
    const b = await tx.query(sql`SELECT 1 + 2 as bar;`);
    return { a, b };
  });
  assert.deepStrictEqual(result, {
    a: [{ foo: 42 }],
    b: [{ bar: 3 }],
  });
});

test("two parallel queries", async (t) => {
  const db = createConnectionPool();
  t.after(db.dispose.bind(db));

  async function query() {
    const [{ foo }] = await db.query(sql`SELECT 1 + 1 as foo`);
    assert.strictEqual(foo, 2);
  }

  await Promise.all([query(), query()]);
});
