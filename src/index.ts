import sql, { SQLQuery, isSqlQuery } from "@databases/sql";
import { createConnectionPool } from "./pool";

export type { SQLQuery };
export { sql, isSqlQuery };

export { createSyncConnection, IN_MEMORY } from "./sync";
export type {
  DatabaseSyncOptions,
  SyncDatabaseConnection,
  SyncDatabaseTransaction,
} from "./sync";

export { createConnectionPool } from "./pool";
export type {
  ConnectionPoolOptions,
  DatabaseConnection,
  DatabaseTransaction,
  OnQueryParameters,
} from "./pool";

export default createConnectionPool;
