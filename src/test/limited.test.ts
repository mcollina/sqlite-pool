import createConnectionPool, {sql} from '../';
import { test } from 'node:test';
import assert from 'node:assert';

async function testPool (t) {
  const db = createConnectionPool(
    ':memory:',
    {},
    {maxSize: 2, releaseTimeoutMilliseconds: 100},
  );
  t.after(db.dispose.bind(db));
  return db;
}

test('two parallel queries', async (t) => {
  const db = await testPool(t);

  let concurrent = 0;
  async function query() {
    const result = await db.tx(async (tx) => {
      if (++concurrent > 2) {
        throw new Error('Too many concurrent queries');
      }
      const a = await tx.query(sql`SELECT 1 + ${41} as ${sql.ident('foo')}`);
      const b = await tx.query(sql`SELECT 1 + 2 as bar;`);
      return {a, b};
    });
    concurrent--;
    assert.deepStrictEqual(result, {a: [{foo: 42}], b: [{bar: 3}]});
  }

  await Promise.all([query(), query(), query(), query()]);
});

test('never releasing', async (t) => {
  const db = await testPool(t);

  await assert.rejects(db.tx(async () => {
    return new Promise(() => {
      // not calling resolve
    });
  }), new Error('Transaction aborted'));
});
