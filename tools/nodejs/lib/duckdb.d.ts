/**
 * TypeScript declarations for Node.JS bindings for DuckDb.
 * See https://duckdb.org/docs/api/nodejs/overview for details
 * on Node.JS API
 */
export class DuckDbError extends Error {
  errno: number;
  code: string;
}

type Callback<T> = (err: DuckDbError | null, res: T) => void;

export type RowData = {
  [columnName: string]: any;
};

export type TableData = RowData[];
export type ArrowIterable = Iterable<Uint8Array> | AsyncIterable<Uint8Array>;
export type ArrowArray = Uint8Array[];

export class Connection {
  constructor(db: Database, callback?: Callback<any>);

  all(sql: string, ...args: [...any, Callback<TableData>] | []): void;
  arrowIPCAll(sql: string, ...args: [...any, Callback<ArrowArray>] | []): void;
  each(sql: string, ...args: [...any, Callback<RowData>] | []): void;
  exec(sql: string, ...args: [...any, Callback<void>] | []): void;

  prepare(sql: string, ...args: [...any, Callback<Statement>] | []): Statement;
  run(sql: string, ...args: [...any, Callback<void>] | []): Statement;

  register_udf(
    name: string,
    return_type: string,
    fun: (...args: any[]) => any
  ): void;

  register_bulk(
    name: string,
    return_type: string,
    fun: (...args: any[]) => any
  ): void;
  unregister_udf(name: string, callback: Callback<any>): void;

  stream(sql: any, ...args: any[]): QueryResult;
  arrowIPCStream(sql: any, ...args: any[]): IpcResultStreamIterator;

  register_buffer(name: string, array: ArrowIterable, force: boolean, callback?: Callback<void>): void;
  unregister_buffer(name: string, callback?: Callback<void>): void;
}

export class QueryResult implements AsyncIterable<RowData> {
  [Symbol.asyncIterator](): AsyncIterator<RowData>;
}

export class IpcResultStreamIterator implements AsyncIterator<Uint8Array>, AsyncIterable<Uint8Array> {
  [Symbol.asyncIterator](): this;

  next(...args: [] | [undefined]): Promise<IteratorResult<Uint8Array, any>>;

  toArray(): Promise<ArrowArray>;
}

export interface ReplacementScanResult {
  function: string;
  parameters: Array<unknown>;
}

export type ReplacementScanCallback = (
  table: string
) => ReplacementScanResult | null;

export class Database {
  constructor(path: string, accessMode?: number | Record<string,string>, callback?: Callback<any>);
  constructor(path: string, callback?: Callback<any>);

  close(callback?: Callback<void>): void;

  connect(): Connection;

  all(sql: string, ...args: [...any, Callback<TableData>] | []): this;
  arrowIPCAll(sql: string, ...args: [...any, Callback<ArrowArray>] | []): void;
  each(sql: string, ...args: [...any, Callback<RowData>] | []): this;
  exec(sql: string, ...args: [...any, Callback<void>] | []): void;

  prepare(sql: string, ...args: [...any, Callback<Statement>] | []): Statement;
  run(sql: string, ...args: [...any, Callback<void>] | []): Statement;

  register_udf(
    name: string,
    return_type: string,
    fun: (...args: any[]) => any
  ): void;
  unregister_udf(name: string, callback: Callback<any>): void;

  stream(sql: any, ...args: any[]): QueryResult;
  arrowIPCStream(sql: any, ...args: any[]): Promise<IpcResultStreamIterator>;

  serialize(done?: Callback<void>): void;
  parallelize(done?: Callback<void>): void;
  wait(done: Callback<void>): void;

  get(columnName: string, cb: Callback<RowData>): void;
  get(columnName: string, num: number, cb: Callback<RowData>): void;

  interrupt(): void;

  register_buffer(name: string, array: ArrowIterable, force: boolean, callback?: Callback<void>): void;

  unregister_buffer(name: string, callback?: Callback<void>): void;

  register_replacement_scan(
    replacementScan: ReplacementScanCallback,
    callback?: Callback<void>
  ): void;
}

export class Statement {
  sql: string;

  constructor(connection: Connection, sql: string);

  all(...args: [...any, Callback<TableData>] | any[]): this;

  arrowIPCAll(...args: [...any, Callback<ArrowArray>] | any[]): void;

  each(...args: [...any, Callback<RowData>] | any[]): this;

  finalize(callback?: Callback<void>): void;

  run(...args: [...any, Callback<void>] | any[]): Statement;
}

export const ERROR: number;

export const OPEN_CREATE: number;

export const OPEN_FULLMUTEX: number;

export const OPEN_PRIVATECACHE: number;

export const OPEN_READONLY: number;

export const OPEN_READWRITE: number;

export const OPEN_SHAREDCACHE: number;

export const INTERRUPT: number;
