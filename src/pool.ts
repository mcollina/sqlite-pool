// Asynchronous connection pool built on top of the synchronous node:sqlite
// client in ./sync, using @databases/connection-pool
// (https://github.com/ForbesLindesay/atdatabases),
// Copyright (c) 2019 Forbes Lindesay, MIT License.

import createBaseConnectionPool, {
  ConnectionPool,
  PoolConnection,
  PoolOptions,
} from "@databases/connection-pool";
import sql, { SQLQuery } from "@databases/sql";
import {
  createSyncConnection,
  sqliteFormat,
  DatabaseSyncOptions,
  SyncDatabaseConnection,
} from "./sync";

export interface DatabaseTransaction {
  query(query: SQLQuery): Promise<any[]>;
  queryStream(query: SQLQuery): AsyncIterableIterator<any>;
}

export interface DatabaseConnection extends DatabaseTransaction {
  tx<T>(fn: (db: DatabaseTransaction) => Promise<T>): Promise<T>;
  dispose(): Promise<void>;
}

export type OnQueryParameters = {
  text: string;
  values: unknown[];
};

export type ConnectionPoolOptions = Omit<
  PoolOptions<SyncDatabaseConnection>,
  "openConnection" | "closeConnection"
> & {
  onQuery?(query: OnQueryParameters): void;
};

interface PooledSyncDatabaseConnection extends SyncDatabaseConnection {
  controller?: AbortController;
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
  maybePoolConnection: Promise<PoolConnection<PooledSyncDatabaseConnection>>,
  query: SQLQuery,
): AsyncIterableIterator<any> {
  const poolConnection = await maybePoolConnection;
  try {
    yield* poolConnection.connection.queryStream(query);
  } finally {
    poolConnection.release();
  }
}

class DatabaseConnectionImplementation implements DatabaseConnection {
  #pool: ConnectionPool<PooledSyncDatabaseConnection>;
  #onQuery: (query: SQLQuery) => void;

  constructor(
    filename?: string,
    options?: DatabaseSyncOptions,
    poolOptions?: ConnectionPoolOptions,
  ) {
    const onQuery = poolOptions?.onQuery;
    this.#onQuery = (query) => {
      if (onQuery) {
        onQuery(query.format(sqliteFormat));
      }
    };
    this.#pool = createBaseConnectionPool<PooledSyncDatabaseConnection>({
      async openConnection() {
        return createSyncConnection(filename, options);
      },
      async closeConnection(connection) {
        connection.dispose();
      },
      async onReleaseTimeout(connection) {
        connection.controller?.abort();
        connection.dispose();
      },
      ...poolOptions,
    });
  }

  async query(query: SQLQuery): Promise<any[]> {
    const poolConnection = await this.#pool.getConnection();
    try {
      this.#onQuery(query);
      return poolConnection.connection.query(query);
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
    const controller = new AbortController();
    const tx = new TransactionImplementation(connection, this.#onQuery);
    try {
      connection.query(sql`BEGIN`);
      connection.controller = controller;
      let onAbort!: () => void;
      const aborted = new Promise<never>((_, reject) => {
        onAbort = () => reject(new Error("Transaction aborted"));
        controller.signal.addEventListener("abort", onAbort, { once: true });
      });
      let res: T;
      try {
        res = await Promise.race([fn(tx), aborted]);
      } finally {
        controller.signal.removeEventListener("abort", onAbort);
      }
      connection.query(sql`COMMIT`);
      return res;
    } catch (e) {
      tx.aborted = true;
      try {
        connection.query(sql`ROLLBACK`);
      } catch {
        // Deliberately swallow this error
      }
      throw e;
    } finally {
      connection.controller = undefined;
      poolConnection.release();
    }
  }

  async dispose(): Promise<void> {
    await this.#pool.drain();
  }
}

/**
 * Create an asynchronous connection pool for a SQLite database.
 * Defaults to an in-memory database.
 */
export function createConnectionPool(
  filename?: string,
  options?: DatabaseSyncOptions,
  poolOptions?: ConnectionPoolOptions,
): DatabaseConnection {
  return new DatabaseConnectionImplementation(filename, options, poolOptions);
}
