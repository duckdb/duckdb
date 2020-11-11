/**
 * Shows how to use chaining rather than the `serialize` method.
 */
"use strict";

var duckdb = require('./lib');
//var duckdb = require('/Users/hannes/source/duckdb/tools/nodejs/build-tmp-napi-v3/Debug/node_duckdb.node');

var db;

function createDb() {
    console.log("createDb chain");
    db = new duckdb.Database(':memory:', createTable);
}


function createTable() {
	/* yay we can have connections too */
	var conn = db.connect()
	conn.all('select 42', function(err, res) {
		console.log(res)
	});

    console.log("createTable lorem");
    //db.run("CREATE TABLE IF NOT EXISTS lorem (info TEXT)", insertRows);
}

function insertRows(err) {
    console.log("insertRows Ipsum i");
    console.log(db)
    var stmt = db.prepare("INSERT INTO lorem VALUES (?)");
    console.log(stmt)
    for (var i = 0; i < 10; i++) {
        stmt.run("Ipsum " + i);
    }

    stmt.finalize(readAllRows);
}

function readAllRows() {
    console.log("readAllRows lorem");
    db.all("SELECT rowid AS id, info FROM lorem", function(err, rows) {
    	console.log(err)
        rows.forEach(function (row) {
            console.log(row.id + ": " + row.info);
        });
        each();
    });
}

var count = 0
function each() {
        console.log("each");
        db.each("SELECT rowid AS id, info FROM lorem", function(err, row) {
            console.log(row)
            count++;
            if (count == 10) {
            	closeDb()
            }
        })
}

function closeDb() {
    console.log("closeDb");
    db.close();
}

function runChainExample() {
    createDb();
}

runChainExample();
