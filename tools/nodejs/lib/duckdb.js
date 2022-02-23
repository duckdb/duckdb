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

function ptr_to_arr(buffer, ptype, n) {
    // TODO can we create those on the C++ side of things already?
    switch(ptype) {
        case 'BOOL':
        case 'UINT8':
            return new Uint8Array(buffer.buffer, 0, n);
        case 'INT8':
            return new Int8Array(buffer.buffer, 0, n);
        case 'INT16':
            return new Int16Array(buffer.buffer, 0, n);
        case 'UINT16':
            return new UInt16Array(buffer.buffer, 0, n);
        case 'INT32':
            return new Int32Array(buffer.buffer, 0, n);
        case 'UINT32':
            return new UInt32Array(buffer.buffer, 0, n);
        case 'FLOAT':
            return new Float32Array(buffer.buffer, 0, n);
        case 'DOUBLE':
            return new Float64Array(buffer.buffer, 0, n);
        case 'VARCHAR':  // we already have created a string array on the C++ side for this
            return buffer;
        default:
            return new Array<string>(0); // cough
    }
}

// this follows the wasm udfs somewhat but is simpler because we can pass data much more cleanly
Connection.prototype.register = function(name, return_type, fun) {
    // TODO what if this throws an error somewhere? do we need a try/catch?
    return this.register_bulk(name, return_type, function(descr) {
        try {
            const data_arr = [];
            const validity_arr = [];

            for (const idx in descr.args) {
                const arg = descr.args[idx];
                validity_arr.push(arg.data_buffers[arg.validity_buffer]);
                data_arr.push(ptr_to_arr(arg.data_buffers[arg.data_buffer], arg.physical_type, descr.rows));
            }

            const out_data = ptr_to_arr(descr.ret.data_buffers[descr.ret.data_buffer], descr.ret.physical_type, descr.rows);
            const out_validity = descr.ret.data_buffers[descr.ret.validity_buffer];

            switch (descr.args.length) {
                case 0:
                    for (let i = 0; i < descr.rows; ++i) {
                        const res = fun();
                        out_data[i] = res;
                        out_validity[i] = res == undefined || res == null ? 0 : 1;
                    }
                    break;
                case 1:
                    for (let i = 0; i < descr.rows; ++i) {
                        const res = fun(validity_arr[0][i] ? data_arr[0][i] : undefined);
                        out_data[i] = res;
                        out_validity[i] = res == undefined || res == null ? 0 : 1;
                    }
                    break;
                case 2:
                    for (let i = 0; i < descr.rows; ++i) {
                        const res = fun(validity_arr[0][i] ? data_arr[0][i] : undefined, validity_arr[1][i] ? data_arr[1][i] : undefined);
                        out_data[i] = res;
                        out_validity[i] = res == undefined || res == null ? 0 : 1;
                    }
                    break;
                case 3:
                    for (let i = 0; i < descr.rows; ++i) {
                        const res = fun(validity_arr[0][i] ? data_arr[0][i] : undefined, validity_arr[1][i] ? data_arr[1][i] : undefined, validity_arr[2][i] ? data_arr[2][i] : undefined);
                        out_data[i] = res;
                        out_validity[i] = res == undefined || res == null ? 0 : 1;
                    }
                    break;
                default:
                    throw "Unsupported argument count";
            }
        } catch(error) { // work around recently fixed napi bug https://github.com/nodejs/node-addon-api/issues/912
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
