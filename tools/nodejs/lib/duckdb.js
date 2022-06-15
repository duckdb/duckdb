var duckdb = require('./duckdb-binding.js');
module.exports = exports = duckdb;


// some wrappers for compatibilities sake
var Database = duckdb.Database;
var Connection = duckdb.Connection;
var Statement = duckdb.Statement;


Connection.prototype.run = function(sql) {
    var statement = new Statement(this, sql);
    return statement.run.apply(statement, arguments);
}

Connection.prototype.all = function(sql) {
    var statement = new Statement(this,sql);
    return statement.all.apply(statement, arguments);
}

Connection.prototype.each = function(sql) {
    var statement = new Statement(this, sql);
    return statement.each.apply(statement, arguments);
}

// this follows the wasm udfs somewhat but is simpler because we can pass data much more cleanly
Connection.prototype.register = function(name, return_type, fun) {
    // TODO what if this throws an error somewhere? do we need a try/catch?
    return this.register_bulk(name, return_type, function(desc) {
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
        } catch(error) { // work around recently fixed napi bug https://github.com/nodejs/node-addon-api/issues/912
            console.log(desc.ret);
            msg = error;
            if (typeof error == 'object' && 'message' in error) {
                msg = error.message
            }
            throw {name: 'DuckDB-UDF-Exception', message : msg};
        }
    })
}

default_connection = function(o) {
    if (o.default_connection == undefined) {
        o.default_connection = new duckdb.Connection(o);
    }
    return(o.default_connection);
}

Database.prototype.prepare = function() {
    return default_connection(this).prepare.apply(this.default_connection, arguments);
}

Database.prototype.run = function() {
    default_connection(this).run.apply(this.default_connection, arguments);
    return this;
}

Database.prototype.each = function() {
    default_connection(this).each.apply(this.default_connection, arguments);
    return this;
}

Database.prototype.all = function() {
    default_connection(this).all.apply(this.default_connection, arguments);
    return this;
}

Database.prototype.exec = function() {
    default_connection(this).exec.apply(this.default_connection, arguments);
    return this;
}

Database.prototype.register = function() {
    default_connection(this).register.apply(this.default_connection, arguments);
    return this;
}

Database.prototype.unregister = function() {
    default_connection(this).unregister.apply(this.default_connection, arguments);
    return this;
}

Database.prototype.get = function() {
    throw "get() is not implemented because it's evil";
}

Statement.prototype.get = function() {
    throw "get() is not implemented because it's evil";
}
