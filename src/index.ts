import createBaseConnectionPool, {
  ConnectionPool,
  PoolConnection,
  PoolOptions,
} from "@databases/connection-pool";
import { escapeSQLiteIdentifier } from "@databases/escape-identifier";
import sql, { SQLQuery, isSqlQuery } from "@databases/sql";
import splitSqlQuery from "@databases/split-sql-query";
import { once } from "events";
import DatabaseConstructor = require("better-sqlite3");

export type { SQLQuery };
export { sql, isSqlQuery };

type DatabaseOptions = DatabaseConstructor.Options;
type BetterSqlite3Database = DatabaseConstructor.Database;

const IN_MEMORY = ":memory:";

// Synchronous adapter vendored from @databases/sqlite-sync@3.0.0.
// Copyright (c) 2019 Forbes Lindesay, MIT License.
const sqliteFormat = {
  escapeIdentifier: (str: string) => escapeSQLiteIdentifier(str),
  formatValue: (value: unknown) => ({ placeholder: "?", value }),
};

interface SyncDatabaseTransaction {
  query(query: SQLQuery): any[];
  query(query: SQLQuery[]): any[][];
  queryStream(query: SQLQuery): IterableIterator<any>;
}

interface SyncDatabaseConnection extends SyncDatabaseTransaction {
  tx<T>(fn: (db: SyncDatabaseTransaction) => T): T;
  dispose(): void;
}

class SyncDatabaseTransactionImplementation implements SyncDatabaseTransaction {
  #database: BetterSqlite3Database;

  constructor(database: BetterSqlite3Database) {
    this.#database = database;
  }

  query(query: SQLQuery): any[];
  query(query: SQLQuery[]): any[][];
  query(query: SQLQuery | SQLQuery[]): any[] | any[][] {
    return runQuery(query, this.#database);
  }

  queryStream(query: SQLQuery): IterableIterator<any> {
    return runQueryStream(query, this.#database);
  }
}

class SyncDatabaseConnectionImplementation implements SyncDatabaseConnection {
  #database: BetterSqlite3Database;
  #begin: DatabaseConstructor.Statement;
  #commit: DatabaseConstructor.Statement;
  #rollback: DatabaseConstructor.Statement;

  constructor(filename: string, options: DatabaseOptions) {
    this.#database = new DatabaseConstructor(filename, options);
    this.#begin = this.#database.prepare("BEGIN");
    this.#commit = this.#database.prepare("COMMIT");
    this.#rollback = this.#database.prepare("ROLLBACK");
  }

  query(query: SQLQuery): any[];
  query(query: SQLQuery[]): any[][];
  query(query: SQLQuery | SQLQuery[]): any[] | any[][] {
    return runQuery(query, this.#database);
  }

  queryStream(query: SQLQuery): IterableIterator<any> {
    return runQueryStream(query, this.#database);
  }

  tx<T>(fn: (db: SyncDatabaseTransaction) => T): T {
    this.#begin.run();
    try {
      const result = fn(
        new SyncDatabaseTransactionImplementation(this.#database),
      );
      this.#commit.run();
      return result;
    } catch (ex) {
      this.#rollback.run();
      throw ex;
    }
  }

  dispose(): void {
    this.#database.close();
  }
}

function connect(
  filename: string = IN_MEMORY,
  options: DatabaseOptions = {},
): SyncDatabaseConnection {
  return new SyncDatabaseConnectionImplementation(filename, options);
}

function runQuery(query: SQLQuery, database: BetterSqlite3Database): any[];
function runQuery(query: SQLQuery[], database: BetterSqlite3Database): any[][];
function runQuery(
  query: SQLQuery | SQLQuery[],
  database: BetterSqlite3Database,
): any[] | any[][];
function runQuery(
  query: SQLQuery | SQLQuery[],
  database: BetterSqlite3Database,
): any[] | any[][] {
  if (Array.isArray(query)) {
    for (const q of query) {
      if (!isSqlQuery(q)) {
        throw new Error(
          "Invalid query, you must use @databases/sql to create your queries.",
        );
      }
    }
    return query.map((q) => runOneQuery(q, database));
  }

  if (!isSqlQuery(query)) {
    throw new Error(
      "Invalid query, you must use @databases/sql to create your queries.",
    );
  }

  const queries = splitSqlQuery(query);
  const lastQuery = queries.pop();
  if (!lastQuery) {
    return [];
  }

  for (const q of queries) {
    runOneQuery(q, database);
  }
  return runOneQuery(lastQuery, database);
}

function runOneQuery(query: SQLQuery, database: BetterSqlite3Database): any[] {
  const { text, values } = query.format(sqliteFormat);
  const statement = database.prepare(text);
  try {
    return statement.all(...values);
  } catch (_err) {
    const err = _err as Error;
    // TODO we should be able to catch this before calling all()
    if (err.message.indexOf("This statement does not return data") >= 0) {
      statement.run(...values);
      return [];
    }
    throw err;
  }
}

function* runQueryStream(
  query: SQLQuery,
  database: BetterSqlite3Database,
): IterableIterator<any> {
  if (!isSqlQuery(query)) {
    throw new Error(
      "Invalid query, you must use @databases/sql to create your queries.",
    );
  }

  const { text, values } = query.format(sqliteFormat);
  const statement = database.prepare(text);
  const rows = statement.iterate(...values);
  for (const row of rows || []) {
    yield row;
  }
}

export interface DatabaseTransaction {
  query(query: SQLQuery): Promise<any[]>;

  queryStream(query: SQLQuery): AsyncIterableIterator<any>;
}

export interface DatabaseConnection extends DatabaseTransaction {
  tx<T>(fn: (db: DatabaseTransaction) => Promise<T>): Promise<T>;
  dispose(): Promise<void>;
}

async function* transactionalQueryStream(
  transaction: TransactionImplementation,
  query: SQLQuery,
): AsyncIterableIterator<any> {
  const connection = transaction.connection;
  for (const row of connection.queryStream(query)) {
    if (transaction.aborted) {
      throw new Error("Transaction aborted");
    }
    yield row;
  }
}

class TransactionImplementation implements DatabaseTransaction {
  connection: SyncDatabaseConnection;
  aborted: boolean = false;
  #onQuery: (query: SQLQuery) => void;

  constructor(
    connection: SyncDatabaseConnection,
    onQuery: (query: SQLQuery) => void,
  ) {
    this.connection = connection;
    this.#onQuery = onQuery;
  }

  async query(query: SQLQuery): Promise<any[]> {
    if (this.aborted) {
      throw new Error("Transaction aborted");
    }
    this.#onQuery(query);
    return this.connection.query(query);
  }

  queryStream(query: SQLQuery): AsyncIterableIterator<any> {
    this.#onQuery(query);
    return transactionalQueryStream(this, query);
  }
}

async function* queryStream(
  maybePoolConnection: Promise<
    PoolConnection<SyncDatabaseConnectionWithController>
  >,
  query: SQLQuery,
) {
  const poolConnection = await maybePoolConnection;
  try {
    for (const row of poolConnection.connection.queryStream(query)) {
      yield row;
    }
  } finally {
    poolConnection.release();
  }
}

type PartialPoolOptions = Omit<
  PoolOptions<SyncDatabaseConnection>,
  "openConnection" | "closeConnection"
>;

type onQueryParamters = {
  text: string;
  values: unknown[];
};

type ConnectionPoolOptions = PartialPoolOptions & {
  onQuery?(onQueryParamters): void;
};

interface SyncDatabaseConnectionWithController extends SyncDatabaseConnection {
  controller?: AbortController;
}

class DatabaseConnectionImplementation implements DatabaseConnection {
  #pool: ConnectionPool<SyncDatabaseConnectionWithController>;
  #onQuery: (query: SQLQuery) => void;

  constructor(
    filename?: string,
    options?: DatabaseOptions,
    poolOptions?: ConnectionPoolOptions,
  ) {
    this.#onQuery = (query) => {
      const formatted = query.format({
        escapeIdentifier: escapeSQLiteIdentifier,
        formatValue: (value) => ({ placeholder: "?", value }),
      });
      poolOptions?.onQuery?.(formatted);
    };
    this.#pool = createBaseConnectionPool({
      async openConnection() {
        return connect(filename, options);
      },
      async closeConnection(connection) {
        connection.dispose();
        return;
      },
      async onReleaseTimeout(connection: SyncDatabaseConnectionWithController) {
        const controller = connection.controller;
        if (controller) {
          controller.abort();
        }
        connection.dispose();
        return;
      },
      ...poolOptions,
    });
  }

  async query(query: SQLQuery): Promise<any[]> {
    const poolConnection = await this.#pool.getConnection();
    try {
      this.#onQuery(query);
      const res = poolConnection.connection.query(query);
      return res;
    } finally {
      poolConnection.release();
    }
  }

  queryStream(query: SQLQuery): AsyncIterableIterator<any> {
    this.#onQuery(query);
    return queryStream(this.#pool.getConnection(), query);
  }

  async tx<T>(fn: (db: DatabaseTransaction) => Promise<T>): Promise<T> {
    const poolConnection = await this.#pool.getConnection();
    const connection = poolConnection.connection;
    try {
      connection.query(sql`BEGIN`);
      const controller = new AbortController();
      const tx = new TransactionImplementation(connection, this.#onQuery);
      connection.controller = controller;
      const res = await Promise.race([
        fn(tx),
        once(controller.signal, "abort").then(() => {
          throw new Error("Transaction aborted");
        }),
      ]);
      connection.query(sql`COMMIT`);
      return res;
    } catch (e) {
      try {
        connection.query(sql`ROLLBACK`);
      } catch {
        // Deliberately swallow this error
      }
      throw e;
    } finally {
      poolConnection.release();
    }
  }

  async dispose(): Promise<void> {
    await this.#pool.drain();
  }
}

export function createConnectionPool(
  filename?: string,
  options?: DatabaseOptions,
  poolOptions?: ConnectionPoolOptions,
): DatabaseConnection {
  return new DatabaseConnectionImplementation(filename, options, poolOptions);
}

export default createConnectionPool;
