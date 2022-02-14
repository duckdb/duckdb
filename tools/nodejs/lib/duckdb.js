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



function type_size(ptype) {
    switch(ptype) {
        case 'UINT8':
        case 'INT8':
            return 1;
        case 'INT32':
        case 'FLOAT':
            return 4;
        case 'INT64':
        case 'UINT64':
        case 'DOUBLE':
            return 8;
        default:
            return 0;
    }
}

function ptr_to_arr(buffer, ptype, n) {

    switch(ptype) {
        case 'UINT8': {
            return new Uint8Array(buffer.buffer, 0, n);
        }
        case 'INT8': {
            return new Int8Array(buffer.buffer, 0, n);
        }
        case 'INT32': {
            return new Int32Array(buffer.buffer, 0, n);
        }
        case 'FLOAT': {
            return new Float32Array(buffer.buffer, 0, n);
        }
        case 'DOUBLE': {
            return new Float64Array(buffer.buffer, 0, n);
        }
        case 'VARCHAR': {
            return new Float64Array(buffer.buffer, 0, n);
        }
        default:
            return new Array<string>(0); // cough
    }
}


// this follows the wasm udfs somewhat
Connection.prototype.register = function(name, fun) {
    return this.register_bulk(name, function(descr, data_buffers) {
       // console.log(descr);

        const data_arr = [];
        const validity_arr = [];

        for (const idx in descr.args) {
            const arg = descr.args[idx];
            data_arr.push(ptr_to_arr(data_buffers[arg.data_buffer], arg.physical_type, descr.rows));
            validity_arr.push(data_buffers[arg.validity_buffer]);
        }
        const out_data = ptr_to_arr(data_buffers[descr.ret.data_buffer], descr.ret.physical_type, descr.rows);
        const out_validity = data_buffers[descr.ret.validity_buffer];

        switch (descr.args.length) {
            case 0:
                for (let i = 0; i < descr.rows; ++i) {
                    const res = fun();
                    out_data[i] = res;
                    out_validity[i] = res == undefined ? 0 : 1;
                }
                break;
            case 1:
                for (let i = 0; i < descr.rows; ++i) {
                    const res = fun(validity_arr[0][i] ? data_arr[0][i] : undefined);
                    out_data[i] = res;
                    out_validity[i] = res == undefined ? 0 : 1;
                }
                break;
            case 2:
                for (let i = 0; i < descr.rows; ++i) {
                    const res = fun(validity_arr[0][i] ? data_arr[0][i] : undefined, validity_arr[1][i] ? data_arr[1][i] : undefined);
                    out_data[i] = res;
                    out_validity[i] = res == undefined ? 0 : 1;
                }
                break;
            default:
                console.log("eeeek");
                // TODO throw an error
                return;
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
    return this.default_connection.prepare.apply(this.default_connection, arguments);
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

Database.prototype.get = function() {
    throw "get() is not implemented because it's evil";
}

Statement.prototype.get = function() {
    throw "get() is not implemented because it's evil";
}
