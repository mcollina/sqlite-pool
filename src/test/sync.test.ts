import { sql, createSyncConnection } from "../";
import { test } from "node:test";
import assert from "node:assert";

function connect(t) {
  const db = createSyncConnection();
  t.after(() => db.dispose());
  return db;
}

test("sync query", (t) => {
  const db = connect(t);
  const [{ foo }] = db.query(sql`SELECT 1 + ${41} as ${sql.ident("foo")}`);
  assert.strictEqual(foo, 42);
});

test("sync error", (t) => {
  const db = connect(t);
  assert.throws(() => db.query(sql`SELECT * FRM 'baz;`));
});

test("sync rejects non-sql queries", (t) => {
  const db = connect(t);
  assert.throws(() => db.query("SELECT 1" as any), /@databases\/sql/);
});

test("sync multiple statements and array of queries", (t) => {
  const db = connect(t);
  const rows = db.query(sql`
    CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);
    INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b');
    SELECT * FROM t ORDER BY id;
  `);
  assert.deepStrictEqual(rows, [
    { id: 1, name: "a" },
    { id: 2, name: "b" },
  ]);
  const [a, b] = db.query([
    sql`SELECT count(*) as c FROM t`,
    sql`SELECT name FROM t WHERE id = ${2}`,
  ]);
  assert.strictEqual(a[0].c, 2);
  assert.strictEqual(b[0].name, "b");
});

test("sync queryStream", (t) => {
  const db = connect(t);
  db.query(sql`CREATE TABLE s (id INTEGER PRIMARY KEY)`);
  db.query(sql`INSERT INTO s (id) VALUES (1), (2), (3)`);
  const ids = [...db.queryStream(sql`SELECT id FROM s ORDER BY id`)].map(
    (r) => r.id,
  );
  assert.deepStrictEqual(ids, [1, 2, 3]);
});

test("sync tx commit and rollback", (t) => {
  const db = connect(t);
  db.query(sql`CREATE TABLE x (id INTEGER PRIMARY KEY)`);
  const result = db.tx((tx) => {
    tx.query(sql`INSERT INTO x (id) VALUES (1)`);
    return tx.query(sql`SELECT count(*) as c FROM x`)[0].c;
  });
  assert.strictEqual(result, 1);
  assert.throws(
    () =>
      db.tx((tx) => {
        tx.query(sql`INSERT INTO x (id) VALUES (2)`);
        throw new Error("boom");
      }),
    /boom/,
  );
  assert.strictEqual(db.query(sql`SELECT count(*) as c FROM x`)[0].c, 1);
});

test("sync dispose is idempotent", () => {
  const db = createSyncConnection();
  db.dispose();
  db.dispose();
});
