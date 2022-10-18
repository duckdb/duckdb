// declare module "duckdb" {
export class Connection {
  constructor(db: Database);

  all(sql: any, ...args: any[]): any;

  each(sql: any, ...args: any[]): any;

  // Native method; no parameter or return type inference available
  exec(): any;

  // Native method; no parameter or return type inference available
  prepare(): any;

  register(name: any, return_type: any, fun: any): any;

  // Native method; no parameter or return type inference available
  register_bulk(): any;

  run(sql: any, ...args: any[]): any;

  stream(sql: any, ...args: any[]): any;

  // Native method; no parameter or return type inference available
  unregister(): any;
}

export class Database {
  constructor(path: string, cb: any);

  all(...args: any[]): any;

  // Native method; no parameter or return type inference available
  close(): any;

  // Native method; no parameter or return type inference available
  connect(): any;

  each(...args: any[]): any;

  exec(...args: any[]): any;

  get(): void;

  // Native method; no parameter or return type inference available
  interrupt(): any;

  // Native method; no parameter or return type inference available
  parallelize(): any;

  prepare(...args: any[]): any;

  register(...args: any[]): any;

  run(...args: any[]): any;

  // Native method; no parameter or return type inference available
  serialize(): any;

  unregister(...args: any[]): any;

  // Native method; no parameter or return type inference available
  wait(): any;
}

export class Statement {
  constructor();

  // Native method; no parameter or return type inference available
  all(): any;

  // Native method; no parameter or return type inference available
  each(): any;

  // Native method; no parameter or return type inference available
  finalize(): any;

  get(): void;

  // Native method; no parameter or return type inference available
  run(): any;
}

export const ERROR: number;

export const OPEN_CREATE: number;

export const OPEN_FULLMUTEX: number;

export const OPEN_PRIVATECACHE: number;

export const OPEN_READONLY: number;

export const OPEN_READWRITE: number;

export const OPEN_SHAREDCACHE: number;
// }
