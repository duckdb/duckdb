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
