/**
 * @module duckdb
 * @summary DuckDB is an embeddable SQL OLAP Database Management System
 */

var duckdb = require('./duckdb-binding.js');
module.exports = exports = duckdb;

/**
 * Check that errno attribute equals this to check for a duckdb error
 * @constant {number}
 */
var ERROR = duckdb.ERROR;

/**
 * Open database in readonly mode
 * @constant {number}
 */
var OPEN_READONLY = duckdb.OPEN_READONLY;
/**
 * Currently ignored
 * @constant {number}
 */
var OPEN_READWRITE = duckdb.OPEN_READWRITE;
/**
 * Currently ignored
 * @constant {number}
 */
var OPEN_CREATE = duckdb.OPEN_CREATE;
/**
 * Currently ignored
 * @constant {number}
 */
var OPEN_FULLMUTEX = duckdb.OPEN_FULLMUTEX;
/**
 * Currently ignored
 * @constant {number}
 */
var OPEN_SHAREDCACHE = duckdb.OPEN_SHAREDCACHE;
/**
 * Currently ignored
 * @constant {number}
 */
var OPEN_PRIVATECACHE = duckdb.OPEN_PRIVATECACHE;

// some wrappers for compatibilities sake
/**
 * Main database interface
 * @arg path - path to database file or :memory: for in-memory database
 * @arg access_mode - access mode
 * @arg config - the configuration object
 * @arg callback - callback function
 */
var Database = duckdb.Database;
/**
 * @class
 */
var Connection = duckdb.Connection;
/**
 * @class
 */
var Statement = duckdb.Statement;
/**
 * @class
 */
var QueryResult = duckdb.QueryResult;

/**
 * @method
 * @return data chunk
 */
QueryResult.prototype.nextChunk;

/**
 * Function to fetch the next result blob of an Arrow IPC Stream in a zero-copy way.
 * (requires arrow extension to be loaded)
 *
 * @method
 * @return data chunk
 */
QueryResult.prototype.nextIpcBuffer;

/**
 * @name asyncIterator
 * @memberof module:duckdb~QueryResult
 * @method
 * @instance
 * @yields data chunks
 */
QueryResult.prototype[Symbol.asyncIterator] = async function*() {
    let prefetch = this.nextChunk();
    while (true) {
        const chunk = await prefetch;
        // Null chunk indicates end of stream
        if (!chunk) {
            return;
        }
        // Prefetch the next chunk while we're iterating
        prefetch = this.nextChunk();
        for (const row of chunk) {
            yield row;
        }
    }
}


/**
 * Run a SQL statement and trigger a callback when done
 * @arg sql
 * @param {...*} params
 * @param callback
 * @return {void}
 */
Connection.prototype.run = function (sql) {
    var statement = new Statement(this, sql);
    return statement.run.apply(statement, arguments);
}

/**
 * Run a SQL query and triggers the callback once for all result rows
 * @arg sql
 * @param {...*} params
 * @param callback
 * @return {void}
 */
Connection.prototype.all = function (sql) {
    var statement = new Statement(this, sql);
    return statement.all.apply(statement, arguments);
}

// Utility class for streaming Apache Arrow IPC
class IpcResultStreamIterator {
    constructor(stream_result_p) {
        this._depleted = false;
        this.stream_result = stream_result_p;
    }

    async next() {
        if (this._depleted) {
            return { done: true, value: null };
        }

        const ipc_raw = await this.stream_result.nextIpcBuffer();
        const res = new Uint8Array(ipc_raw);

        this._depleted = res.length == 0;
        return {
            done: this._depleted,
            value: res,
        };
    }

    [Symbol.asyncIterator]() {
        return this;
    }

    // Materialize the IPC stream into a list of Uint8Arrays
    async toArray () {
        const retval = []

        for await (const ipc_buf of this) {
            retval.push(ipc_buf);
        }

        // Push EOS message containing 4 bytes of 0
        retval.push(new Uint8Array([0,0,0,0]));

        return retval;
    }
}

/**
 * Run a SQL query and serialize the result into the Apache Arrow IPC format (requires arrow extension to be loaded)
 * @arg sql
 * @param {...*} params
 * @param callback
 * @return {void}
 */
Connection.prototype.arrowIPCAll = function (sql) {
    const query = "SELECT * FROM to_arrow_ipc((" + sql + "));";
    var statement = new Statement(this, query);
    return statement.arrowIPCAll.apply(statement, arguments);
}

/**
 * Run a SQL query, returns a IpcResultStreamIterator that allows streaming the result into the Apache Arrow IPC format
 * (requires arrow extension to be loaded)
 *
 * @arg sql
 * @param {...*} params
 * @param callback
 * @return Promise<IpcResultStreamIterator>
 */
Connection.prototype.arrowIPCStream = async function (sql) {
    const query = "SELECT * FROM to_arrow_ipc((" + sql + "));";
    const statement = new Statement(this, query);
    return new IpcResultStreamIterator(await statement.stream.apply(statement, arguments));
}

/**
 * Runs a SQL query and triggers the callback for each result row
 * @arg sql
 * @param {...*} params
 * @param callback
 * @return {void}
 */
Connection.prototype.each = function (sql) {
    var statement = new Statement(this, sql);
    return statement.each.apply(statement, arguments);
}

/**
 * @arg sql
 * @param {...*} params
 * @yields row chunks
 */
Connection.prototype.stream = async function* (sql) {
    const statement = new Statement(this, sql);
    const queryResult = await statement.stream.apply(statement, arguments);
    for await (const result of queryResult) {
        yield result;
    }
}

/**
 * Register a User Defined Function
 *
 * @arg name
 * @arg return_type
 * @arg fun
 * @return {void}
 * @note this follows the wasm udfs somewhat but is simpler because we can pass data much more cleanly
 */
Connection.prototype.register_udf = function (name, return_type, fun) {
    // TODO what if this throws an error somewhere? do we need a try/catch?
    return this.register_udf_bulk(name, return_type, function (desc) {
        try {
            // Build an argument resolver
            const buildResolver = (arg) => {
                let validity = arg.validity || null;
                switch (arg.physicalType) {
                    case 'STRUCT': {
                        const tmp = {};
                        const children = [];
                        for (let j = 0; j < (arg.children.length || 0); ++j) {
                            const attr = arg.children[j];
                            const child = buildResolver(attr);
                            children.push((row) => {
                                tmp[attr.name] = child(row);
                            });
                        }
                        if (validity != null) {
                            return (row) => {
                                if (!validity[row]) {
                                    return null;
                                }
                                for (const resolver of children) {
                                    resolver(row);
                                }
                                return tmp;
                            };
                        } else {
                            return (row) => {
                                for (const resolver of children) {
                                    resolver(row);
                                }
                                return tmp;
                            };
                        }
                    }
                    default: {
                        if (arg.data === undefined) {
                            throw new Error(
                                'malformed data view, expected data buffer for argument of type: ' + arg.physicalType,
                            );
                        }
                        const data = arg.data;
                        if (validity != null) {
                            return (row) => (!validity[row] ? null : data[row]);
                        } else {
                            return (row) => data[row];
                        }
                    }
                }
            };

            // Translate argument data
            const argResolvers = [];
            for (let i = 0; i < desc.args.length; ++i) {
                argResolvers.push(buildResolver(desc.args[i]));
            }
            const args = [];
            for (let i = 0; i < desc.args.length; ++i) {
                args.push(null);
            }

            // Return type
            desc.ret.validity = new Uint8Array(desc.rows);
            switch (desc.ret.physicalType) {
                case 'INT8':
                    desc.ret.data = new Int8Array(desc.rows);
                    break;
                case 'INT16':
                    desc.ret.data = new Int16Array(desc.rows);
                    break;
                case 'INT32':
                    desc.ret.data = new Int32Array(desc.rows);
                    break;
                case 'DOUBLE':
                    desc.ret.data = new Float64Array(desc.rows);
                    break;
                case 'DATE64':
                case 'TIME64':
                case 'TIMESTAMP':
                case 'INT64':
                    desc.ret.data = new BigInt64Array(desc.rows);
                    break;
                case 'UINT64':
                    desc.ret.data = new BigUint64Array(desc.rows);
                    break;
                case 'BLOB':
                case 'VARCHAR':
                    desc.ret.data = new Array(desc.rows);
                    break;
            }

            // Call the function
            for (let i = 0; i < desc.rows; ++i) {
                for (let j = 0; j < desc.args.length; ++j) {
                    args[j] = argResolvers[j](i);
                }
                const res = fun(...args);
                desc.ret.data[i] = res;
                desc.ret.validity[i] = res === undefined || res === null ? 0 : 1;
            }
        } catch (error) { // work around recently fixed napi bug https://github.com/nodejs/node-addon-api/issues/912
            msg = error;
            if (typeof error == 'object' && 'message' in error) {
                msg = error.message
            }
            throw { name: 'DuckDB-UDF-Exception', message: msg };
        }
    })
}

/**
 * Prepare a SQL query for execution
 * @method
 * @arg sql
 * @param {...*} params
 * @param callback
 * @return {Statement}
 */
Connection.prototype.prepare;
/**
 * Execute a SQL query
 * @method
 * @arg sql
 * @param {...*} params
 * @param callback
 * @return {void}
 */
Connection.prototype.exec;
/**
 * Register a User Defined Function
 *
 * @method
 * @arg name
 * @arg return_type
 * @param callback
 * @return {void}
 */
Connection.prototype.register_udf_bulk;
/**
 * Unregister a User Defined Function
 *
 * @method
 * @arg name
 * @arg return_type
 * @param callback
 * @return {void}
 */
Connection.prototype.unregister_udf;

var default_connection = function (o) {
    if (o.default_connection == undefined) {
        o.default_connection = new duckdb.Connection(o);
    }
    return o.default_connection;
}

/**
 * Register a Buffer to be scanned using the Apache Arrow IPC scanner
 * (requires arrow extension to be loaded)
 *
 * @method
 * @arg name
 * @arg array
 * @arg force
 * @param callback
 * @return {void}
 */
Connection.prototype.register_buffer;

/**
 * Unregister the Buffer
 *
 * @method
 * @arg name
 * @param callback
 * @return {void}
 */
Connection.prototype.unregister_buffer;


/**
 * Closes database instance
 * @method
 * @param callback
 * @return {void}
 */
Database.prototype.close = function() {
    this.default_connection = null
    this.close_internal.apply(this, arguments);
};

/**
 * Internal method. Do not use, call Connection#close instead
 * @method
 * @param callback
 * @return {void}
 */
Database.prototype.close_internal;

/**
 * Triggers callback when all scheduled database tasks have completed.
 * @method
 * @param callback
 * @return {void}
 */
Database.prototype.wait;

/**
 * Currently a no-op. Provided for SQLite compatibility
 * @method
 * @param callback
 * @return {void}
 */
Database.prototype.serialize;

/**
 * Currently a no-op. Provided for SQLite compatibility
 * @method
 * @param callback
 * @return {void}
 */
Database.prototype.parallelize;

/**
 * Create a new database connection
 * @method
 * @arg path the database to connect to, either a file path, or `:memory:`
 * @return {Connection}
 */
Database.prototype.connect;

/**
 * Supposedly interrupt queries, but currently does not do anything.
 * @method
 * @param callback
 * @return {void}
 */
Database.prototype.interrupt;

/**
 * Prepare a SQL query for execution
 * @arg sql
 * @return {Statement}
 */
Database.prototype.prepare = function () {
    return default_connection(this).prepare.apply(this.default_connection, arguments);
}

/**
 * Convenience method for Connection#run using a built-in default connection
 * @arg sql
 * @param {...*} params
 * @param callback
 * @return {void}
 */
Database.prototype.run = function () {
    default_connection(this).run.apply(this.default_connection, arguments);
    return this;
}

/**
 * Convenience method for Connection#scanArrowIpc using a built-in default connection
 * @arg sql
 * @param {...*} params
 * @param callback
 * @return {void}
 */
Database.prototype.scanArrowIpc = function () {
    default_connection(this).scanArrowIpc.apply(this.default_connection, arguments);
    return this;
}

/**
 * @arg sql
 * @param {...*} params
 * @param callback
 * @return {void}
 */
Database.prototype.each = function () {
    default_connection(this).each.apply(this.default_connection, arguments);
    return this;
}

/**
 * Convenience method for Connection#apply using a built-in default connection
 * @arg sql
 * @param {...*} params
 * @param callback
 * @return {void}
 */
Database.prototype.all = function () {
    default_connection(this).all.apply(this.default_connection, arguments);
    return this;
}

/**
 * Convenience method for Connection#arrowIPCAll using a built-in default connection
 * @arg sql
 * @param {...*} params
 * @param callback
 * @return {void}
 */
Database.prototype.arrowIPCAll = function () {
    default_connection(this).arrowIPCAll.apply(this.default_connection, arguments);
    return this;
}

/**
 * Convenience method for Connection#arrowIPCStream using a built-in default connection
 * @arg sql
 * @param {...*} params
 * @param callback
 * @return {void}
 */
Database.prototype.arrowIPCStream = function () {
    return default_connection(this).arrowIPCStream.apply(this.default_connection, arguments);
}

/**
 * @arg sql
 * @param {...*} params
 * @param callback
 * @return {void}
 */
Database.prototype.exec = function () {
    default_connection(this).exec.apply(this.default_connection, arguments);
    return this;
}

/**
 * Register a User Defined Function
 *
 * Convenience method for Connection#register_udf
 * @arg name
 * @arg return_type
 * @arg fun
 * @return {this}
 */
Database.prototype.register_udf = function () {
    default_connection(this).register_udf.apply(this.default_connection, arguments);
    return this;
}

/**
 * Register a buffer containing serialized data to be scanned from DuckDB.
 *
 * Convenience method for Connection#unregister_buffer
 * @arg name
 * @return {this}
 */
Database.prototype.register_buffer = function () {
    default_connection(this).register_buffer.apply(this.default_connection, arguments);
    return this;
}

/**
 * Unregister a Buffer
 *
 * Convenience method for Connection#unregister_buffer
 * @arg name
 * @return {this}
 */
Database.prototype.unregister_buffer = function () {
    default_connection(this).unregister_buffer.apply(this.default_connection, arguments);
    return this;
}

/**
 * Unregister a UDF
 *
 * Convenience method for Connection#unregister_udf
 * @arg name
 * @return {this}
 */
Database.prototype.unregister_udf = function () {
    default_connection(this).unregister_udf.apply(this.default_connection, arguments);
    return this;
}

/**
 * Register a table replace scan function
 * @method
 * @arg fun Replacement scan function
 * @return {this}
 */

Database.prototype.registerReplacementScan;

/**
 * Not implemented
 */
Database.prototype.get = function () {
    throw "get() is not implemented because it's evil";
}

/**
 * Not implemented
 */
Statement.prototype.get = function () {
    throw "get() is not implemented because it's evil";
}

/**
 * @method
 * @arg sql
 * @param {...*} params
 * @param callback
 * @return {void}
 */
Statement.prototype.run;
/**
 * @method
 * @arg sql
 * @param {...*} params
 * @param callback
 * @return {void}
 */
Statement.prototype.all;
/**
 * @method
 * @arg sql
 * @param {...*} params
 * @param callback
 * @return {void}
 */
Statement.prototype.arrowIPCAll;
/**
 * @method
 * @arg sql
 * @param {...*} params
 * @param callback
 * @return {void}
 */
Statement.prototype.each;
/**
 * @method
 * @arg sql
 * @param {...*} params
 * @param callback
 * @return {void}
 */
Statement.prototype.finalize
/**
 * @method
 * @arg sql
 * @param {...*} params
 * @yield callback
 */
Statement.prototype.stream;

/**
 * @field
 * @returns sql contained in statement
 */
Statement.prototype.sql;

/**
 * @method
 * @return {ColumnInfo[]} - Array of column names and types
 */
Statement.prototype.columns;

/**
 * @typedef ColumnInfo
 * @type {object}
 * @property {string} name - Column name
 * @property {TypeInfo} type - Column type
 */

/**
 * @typedef TypeInfo
 * @type {object}
 * @property {string} id - Type ID
 * @property {string} [alias] - SQL type alias
 * @property {string} sql_type - SQL type name
 */

/**
 * @typedef DuckDbError
 * @type {object}
 * @property {number} errno - -1 for DuckDB errors
 * @property {string} message - Error message
 * @property {string} code - 'DUCKDB_NODEJS_ERROR' for DuckDB errors
 * @property {string} errorType - DuckDB error type code (eg, HTTP, IO, Catalog)
 */

/**
 * @typedef HTTPError
 * @type {object}
 * @extends {DuckDbError}
 * @property {number} statusCode - HTTP response status code
 * @property {string} reason - HTTP response reason
 * @property {string} response - HTTP response body
 * @property {object} headers - HTTP headers
 */
