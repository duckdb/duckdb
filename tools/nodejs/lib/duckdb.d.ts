/**
 * TypeScript declarations for Node.JS bindings for DuckDb.
 * See https://duckdb.org/docs/api/nodejs/overview for details
 * on Node.JS API
 */

export type ExceptionType =
    | "Invalid"          // invalid type
    | "Out of Range"     // value out of range error
    | "Conversion"       // conversion/casting error
    | "Unknown Type"     // unknown type
    | "Decimal"          // decimal related
    | "Mismatch Type"    // type mismatch
    | "Divide by Zero"   // divide by 0
    | "Object Size"      // object size exceeded
    | "Invalid type"     // incompatible for operation
    | "Serialization"    // serialization
    | "TransactionContext" // transaction management
    | "Not implemented"  // method not implemented
    | "Expression"       // expression parsing
    | "Catalog"          // catalog related
    | "Parser"           // parser related
    | "Binder"           // binder related
    | "Planner"          // planner related
    | "Scheduler"        // scheduler related
    | "Executor"         // executor related
    | "Constraint"       // constraint related
    | "Index"            // index related
    | "Stat"             // stat related
    | "Connection"       // connection related
    | "Syntax"           // syntax related
    | "Settings"         // settings related
    | "Optimizer"        // optimizer related
    | "NullPointer"      // nullptr exception
    | "IO"               // IO exception
    | "INTERRUPT"        // interrupt
    | "FATAL"            // Fatal exceptions are non-recoverable and render the entire DB in an unusable state
    | "INTERNAL"         // Internal exceptions indicate something went wrong internally (i.e. bug in the code base)
    | "Invalid Input"    // Input or arguments error
    | "Out of Memory"    // out of memory
    | "Permission"       // insufficient permissions
    | "Parameter Not Resolved" // parameter types could not be resolved
    | "Parameter Not Allowed"  // parameter types not allowed
    | "Dependency"       // dependency
    | "Unknown"
    | "HTTP"
    ;

/**
 * Standard error shape for DuckDB errors
 */
export interface _DuckDbError extends Error {
  errno: -1; // value of ERROR
  code: 'DUCKDB_NODEJS_ERROR';
  errorType: ExceptionType;
}

export interface HttpError extends _DuckDbError {
  errorType: 'HTTP';
  statusCode: number;
  response: string;
  reason: string;
  headers: Record<string, string>;
}

export type DuckDbError = HttpError | _DuckDbError;

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
  arrowIPCStream(sql: any, ...args: any[]): Promise<IpcResultStreamIterator>;

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

  registerReplacementScan(
    replacementScan: ReplacementScanCallback
  ): Promise<void>;
}

export type GenericTypeInfo = {
  id: string,
  sql_type: string,
  alias?: string,
}

export type StructTypeInfo = {
  id: "STRUCT",
  alias?: string,
  sql_type: string,
  children: TypeInfoChildren,
}

export type ListTypeInfo = {
  id: "LIST",
  alias?: string,
  sql_type: string,
  child: TypeInfo,
}

export type MapTypeInfo = {
  id: "MAP",
  alias?: string,
  sql_type: string,
  key: TypeInfo,
  value: TypeInfo,
}

export type UnionTypeInfo = {
  id: "UNION",
  alias?: string,
  sql_type: string,
  children: TypeInfoChildren,
}

export type DecimalTypeInfo = {
  id: "DECIMAL",
  alias?: string,
  sql_type: string,
  width: number,
  scale: number,
}

export type EnumTypeInfo = {
  id: "ENUM",
  alias?: string,
  sql_type: string,
  name: string,
  values: string[],
}

export type TypeInfoChildren = { name: string, type: TypeInfo }[];

export type TypeInfo = GenericTypeInfo | StructTypeInfo | ListTypeInfo | MapTypeInfo | UnionTypeInfo | DecimalTypeInfo | EnumTypeInfo;

export type ColumnInfo = { name: string, type: TypeInfo };

export class Statement {
  sql: string;

  constructor(connection: Connection, sql: string);

  all(...args: [...any, Callback<TableData>] | any[]): this;

  arrowIPCAll(...args: [...any, Callback<ArrowArray>] | any[]): void;

  each(...args: [...any, Callback<RowData>] | any[]): this;

  finalize(callback?: Callback<void>): void;

  run(...args: [...any, Callback<void>] | any[]): Statement;

  columns(): ColumnInfo[];
}

export const ERROR: number;

export const OPEN_CREATE: number;

export const OPEN_FULLMUTEX: number;

export const OPEN_PRIVATECACHE: number;

export const OPEN_READONLY: number;

export const OPEN_READWRITE: number;

export const OPEN_SHAREDCACHE: number;

export const INTERRUPT: number;
