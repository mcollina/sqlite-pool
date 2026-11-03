// Synchronous SQLite client built on node:sqlite.
//
// Adapted from @databases/sqlite-sync@3.0.0
// (https://github.com/ForbesLindesay/atdatabases/tree/master/packages/sqlite-sync),
// Copyright (c) 2019 Forbes Lindesay, MIT License.
// Ported from better-sqlite3 to node:sqlite.

import { DatabaseSync, StatementSync } from "node:sqlite";
import type { DatabaseSyncOptions } from "node:sqlite";
import { escapeSQLiteIdentifier } from "@databases/escape-identifier";
import { SQLQuery, isSqlQuery } from "@databases/sql";
import splitSqlQuery from "@databases/split-sql-query";

export type { DatabaseSyncOptions };

export const IN_MEMORY = ":memory:";

export const sqliteFormat = {
  escapeIdentifier: (str: string) => escapeSQLiteIdentifier(str),
  formatValue: (value: unknown) => ({ placeholder: "?", value }),
};

export interface SyncDatabaseTransaction {
  query(query: SQLQuery): any[];
  query(query: SQLQuery[]): any[][];
  queryStream(query: SQLQuery): IterableIterator<any>;
}

export interface SyncDatabaseConnection extends SyncDatabaseTransaction {
  tx<T>(fn: (db: SyncDatabaseTransaction) => T): T;
  dispose(): void;
}

class SyncDatabaseTransactionImplementation implements SyncDatabaseTransaction {
  #database: DatabaseSync;

  constructor(database: DatabaseSync) {
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
  #database: DatabaseSync;
  #begin: StatementSync;
  #commit: StatementSync;
  #rollback: StatementSync;
  #disposed = false;

  constructor(filename: string, options: DatabaseSyncOptions) {
    this.#database = new DatabaseSync(filename, options);
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
    // node:sqlite throws when closing an already closed database,
    // better-sqlite3 did not. Keep dispose() idempotent.
    if (this.#disposed) {
      return;
    }
    this.#disposed = true;
    this.#database.close();
  }
}

/**
 * Open a synchronous connection to a SQLite database.
 * Defaults to an in-memory database.
 */
export function createSyncConnection(
  filename: string = IN_MEMORY,
  options: DatabaseSyncOptions = {},
): SyncDatabaseConnection {
  return new SyncDatabaseConnectionImplementation(filename, options);
}

function assertSqlQuery(query: unknown): asserts query is SQLQuery {
  if (!isSqlQuery(query)) {
    throw new Error(
      "Invalid query, you must use @databases/sql to create your queries.",
    );
  }
}

function runQuery(query: SQLQuery, database: DatabaseSync): any[];
function runQuery(query: SQLQuery[], database: DatabaseSync): any[][];
function runQuery(
  query: SQLQuery | SQLQuery[],
  database: DatabaseSync,
): any[] | any[][];
function runQuery(
  query: SQLQuery | SQLQuery[],
  database: DatabaseSync,
): any[] | any[][] {
  if (Array.isArray(query)) {
    query.forEach(assertSqlQuery);
    return query.map((q) => runOneQuery(q, database));
  }

  assertSqlQuery(query);

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

function runOneQuery(query: SQLQuery, database: DatabaseSync): any[] {
  const { text, values } = query.format(sqliteFormat);
  // Unlike better-sqlite3, node:sqlite's all() returns an empty array for
  // statements that do not return data, so no run() fallback is needed.
  return database
    .prepare(text)
    .all(...(values as any[]))
    .map(toPlainObject);
}

function* runQueryStream(
  query: SQLQuery,
  database: DatabaseSync,
): IterableIterator<any> {
  assertSqlQuery(query);
  const { text, values } = query.format(sqliteFormat);
  for (const row of database.prepare(text).iterate(...(values as any[]))) {
    yield toPlainObject(row);
  }
}

// node:sqlite returns rows with a null prototype; better-sqlite3 returned
// plain objects. Convert so results keep working with hasOwnProperty(),
// deepStrictEqual() and similar.
function toPlainObject(row: object): any {
  return { ...row };
}
