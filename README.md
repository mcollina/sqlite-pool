# sqlite-pool

The `@matteo.collina/sqlite-pool` library provides a safe and convenient API for
querying SQLite databases in Node.js, built on the built-in
[`node:sqlite`](https://nodejs.org/api/sqlite.html) module. No native addons
are required.

It exports two implementations:

- `createSyncConnection()`: a synchronous client for a single connection.
- `createConnectionPool()`: an asynchronous client backed by a pool of connections.

Both accept queries built with [`@databases/sql`](https://www.atdatabases.org/docs/sql),
which is re-exported as `sql`.

Requires Node.js 22.13.0 or later. `node:sqlite` is still marked experimental in
Node.js, so it may print an `ExperimentalWarning` on first use.

When using this module, consider that:

> SQLite supports multiple simultaneous read transactions coming from separate database
connections, possibly in separate threads or processes, but only one simultaneous
write transaction - [source](https://www.sqlite.org/lang_transaction.html).

## Usage

### Connection pool (async)

```typescript
import { sql, createConnectionPool } from '@matteo.collina/sqlite-pool';
// or in CommonJS:
// const { createConnectionPool, sql } = require('@matteo.collina/sqlite-pool');

const db = createConnectionPool('my-database.db');

const results = await db.query(sql`SELECT * FROM users;`);
console.log(results);

await db.dispose();
```

### Sync connection

```typescript
import { sql, createSyncConnection } from '@matteo.collina/sqlite-pool';

const db = createSyncConnection('my-database.db');

const results = db.query(sql`SELECT * FROM users;`);
console.log(results);

db.dispose();
```

> For details on how to build queries, see [Building SQL Queries](https://www.atdatabases.org/docs/sql)

Rows come back as plain JavaScript objects. Integer columns are returned as
`number` unless you pass `{ readBigInts: true }` in the options.

## API

### `createConnectionPool(filename?, options?, poolOptions?)`

Create a connection pool for a database. You should only create one pool per
database for your entire application. Normally this means having one module
that creates and exports the pool. It is also the default export.

- `filename`: path to the database file. Defaults to `':memory:'`. Each pooled
  connection to `':memory:'` opens its own separate in-memory database.
- `options`: [`DatabaseSync` options](https://nodejs.org/api/sqlite.html#new-databasesyncpath-options)
  passed to each connection (`readOnly`, `timeout`, `readBigInts`, …).
- `poolOptions`: options for [`@databases/connection-pool`](https://www.npmjs.com/package/@databases/connection-pool)
  (`maxSize`, `releaseTimeoutMilliseconds`, …), plus:
  - `onQuery({ text, values })`: called with every query before it runs.

In memory:

```ts
import createConnectionPool from '@matteo.collina/sqlite-pool';

const db = createConnectionPool();
```

File system:

```ts
import createConnectionPool from '@matteo.collina/sqlite-pool';

const db = createConnectionPool(FILE_NAME);
```

`DatabaseConnection` extends `DatabaseTransaction`, so you can call
`DatabaseConnection.query` directly instead of creating a transaction for every
query. SQLite has limited support for concurrent transactions, so only use
transactions when you need them.

#### `DatabaseConnection.query(SQLQuery): Promise<any[]>`

Run an SQL query and get a promise for an array of results. A query containing
several statements runs each one and returns the results of the last.

#### `DatabaseConnection.queryStream(SQLQuery): AsyncIterable<any>`

Run an SQL query and get an async iterable of the results. e.g.

```js
for await (const record of db.queryStream(sql`SELECT * FROM massive_table`)) {
  console.log(record);
}
```

#### `DatabaseConnection.tx(fn): Promise<T>`

Executes a callback function as a transaction on a single pooled connection.

1. it executes `BEGIN` just before invoking the callback function
2. it executes `COMMIT`, if the callback didn't throw any error or return a rejected promise
3. it executes `ROLLBACK`, if the callback did throw an error or return a rejected promise

If `releaseTimeoutMilliseconds` is set and the callback takes longer, the
transaction rejects with `Transaction aborted` and the connection is closed.

```ts
const result = await db.tx(async (transaction) => {
  const resultA = await transaction.query(sql`SELECT 1 + 1 AS a`);
  const resultB = await transaction.query(sql`SELECT 1 + 1 AS b`);
  return resultA[0].a + resultB[0].b;
});
// => 4
```

#### `DatabaseConnection.dispose(): Promise<void>`

Dispose the pool. Once this is called, any subsequent queries will fail.

### `createSyncConnection(filename?, options?)`

Open a single synchronous connection. `filename` defaults to `':memory:'` and
`options` are passed to [`DatabaseSync`](https://nodejs.org/api/sqlite.html#new-databasesyncpath-options).

#### `SyncDatabaseConnection.query(SQLQuery | SQLQuery[]): any[] | any[][]`

Run an SQL query and return an array of results. If you pass an array of
queries, it returns an array with the results of each query.

#### `SyncDatabaseConnection.queryStream(SQLQuery): Iterable<any>`

Run an SQL query and get an iterable of the results.

#### `SyncDatabaseConnection.tx(fn): T`

Runs `fn` inside `BEGIN`/`COMMIT`, rolling back if it throws. `fn` must be
synchronous.

```ts
const count = db.tx((tx) => {
  tx.query(sql`INSERT INTO users (name) VALUES (${'alice'})`);
  return tx.query(sql`SELECT count(*) AS c FROM users`)[0].c;
});
```

#### `SyncDatabaseConnection.dispose(): void`

Close the connection. Calling it more than once is a no-op.

## Credits

The sync client is adapted from
[`@databases/sqlite-sync`](https://github.com/ForbesLindesay/atdatabases/tree/master/packages/sqlite-sync),
and the pool is built on `@databases/connection-pool`, `@databases/sql`,
`@databases/split-sql-query` and `@databases/escape-identifier`, all by
Forbes Lindesay under the MIT license. See [LICENSE](LICENSE).

## License

MIT
