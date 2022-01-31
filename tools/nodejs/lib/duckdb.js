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

Database.prototype.prepare = function() {
    if (this.default_connection == undefined) {
        this.default_connection = new duckdb.Connection(this);
    }
    return this.default_connection.prepare.apply(this.default_connection, arguments);
}

Database.prototype.run = function() {
    if (this.default_connection == undefined) {
        this.default_connection = new Connection(this);
    }
    this.default_connection.run.apply(this.default_connection, arguments);
    return this;
}

Database.prototype.each = function() {
    if (this.default_connection == undefined) {
        this.default_connection = new Connection(this);
    }
    this.default_connection.each.apply(this.default_connection, arguments);
    return this;
}

Database.prototype.all = function() {
    if (this.default_connection == undefined) {
        this.default_connection = new Connection(this);
    }
    this.default_connection.all.apply(this.default_connection, arguments);
    return this;
}

Database.prototype.exec = function() {
    if (this.default_connection == undefined) {
        this.default_connection = new Connection(this);
    }
    this.default_connection.exec.apply(this.default_connection, arguments);
    return this;
}

Database.prototype.register = function() {
    if (this.default_connection == undefined) {
        this.default_connection = new Connection(this);
    }
    this.default_connection.register.apply(this.default_connection, arguments);
    return this;
}


Database.prototype.get = function() {
    throw "get() is not implemented because it's evil";
}

Statement.prototype.get = function() {
    throw "get() is not implemented because it's evil";
}
