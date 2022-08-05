/**
 * @module duckdb
 * @summary these jsdoc annotations are still a work in progress - feedback and suggestions are welcome!
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
 * @arg sql
 * @param {...*} params
 * @param callback
 * @return {void}
 */
Connection.prototype.all = function (sql) {
    var statement = new Statement(this, sql);
    return statement.all.apply(statement, arguments);
}

/**
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
Connection.prototype.register = function (name, return_type, fun) {
    // TODO what if this throws an error somewhere? do we need a try/catch?
    return this.register_bulk(name, return_type, function (desc) {
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
            console.log(desc.ret);
            msg = error;
            if (typeof error == 'object' && 'message' in error) {
                msg = error.message
            }
            throw { name: 'DuckDB-UDF-Exception', message: msg };
        }
    })
}

/**
 * @method
 * @arg sql
 * @param {...*} params
 * @param callback
 * @return {Statement}
 */
Connection.prototype.prepare;
/**
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
Connection.prototype.register_bulk;
/**
 * Unregister a User Defined Function
 *
 * @method
 * @arg name
 * @arg return_type
 * @param callback
 * @return {void}
 */
Connection.prototype.unregister;

default_connection = function (o) {
    if (o.default_connection == undefined) {
        o.default_connection = new duckdb.Connection(o);
    }
    return o.default_connection;
}


/**
 * @method
 * @param callback
 * @return {void}
 */
Database.prototype.close;

/**
 * @method
 * @param callback
 * TODO: what does this do?
 * @return {void}
 */
Database.prototype.wait;

/**
 * TODO: what does this do?
 * @method
 * @param callback
 * @return {void}
 */
Database.prototype.serialize;

/**
 * TODO: what does this do?
 * @method
 * @param callback
 * @return {void}
 */
Database.prototype.parallelize;

/**
 * @method
 * @arg path the database to connect to, either a file path, or `:memory:`
 * @return {Connection}
 */
Database.prototype.connect;

/**
 * TODO: what does this do?
 * @method
 * @param callback
 * @return {void}
 */
Database.prototype.interrupt;

/**
 * @arg sql
 * @return {Statement}
 */
Database.prototype.prepare = function () {
    return default_connection(this).prepare.apply(this.default_connection, arguments);
}

/**
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
 * Convenience method for Connection#register
 * @arg name
 * @arg return_type
 * @arg fun
 * @return {this}
 */
Database.prototype.register = function () {
    default_connection(this).register.apply(this.default_connection, arguments);
    return this;
}

/**
 * Unregister a User Defined Function
 *
 * Convenience method for Connection#unregister
 * @arg name
 * @return {this}
 */
Database.prototype.unregister = function () {
    default_connection(this).unregister.apply(this.default_connection, arguments);
    return this;
}

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
