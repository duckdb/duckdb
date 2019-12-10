#define CATCH_CONFIG_MAIN
#include "catch.hpp"
#include "sqlite3.h"
#include <string>

using namespace std;

static int concatenate_results(void* arg, int ncols, char** vals, char** colnames) {
	auto &results = *((vector<vector<string>> *) arg);
	if (results.size() == 0) {
		results.resize(ncols);
	}
	for(int i = 0; i < ncols; i++) {
		results[i].push_back(vals[i]);
	}
	return SQLITE_OK;
}

// C++ wrapper class for the C wrapper API that wraps our C++ API, because why not
class SQLiteDBWrapper {
public:
	SQLiteDBWrapper() :
		db(nullptr) {
	}
	~SQLiteDBWrapper() {
		if (db) {
			sqlite3_close(db);
		}
	}

	sqlite3 *db;
	vector<vector<string>> results;
public:
	int Open(string filename) {
		return sqlite3_open(filename.c_str(), &db) == SQLITE_OK;
	}

	string GetErrorMessage() {
		auto err = sqlite3_errmsg(db);
		return err ? string(err) : string();
	}

	bool Execute(string query) {
		results.clear();
		char *errmsg = nullptr;
		int rc = sqlite3_exec(db, query.c_str(), concatenate_results, &results, &errmsg);
		if (errmsg) {
			fprintf(stderr, "%s\n", errmsg);
			sqlite3_free(errmsg);
		}
		return rc == SQLITE_OK;
	}

	bool CheckColumn(size_t column, vector<string> expected_data) {
		if (column >= results.size()) {
			fprintf(stderr, "Column index is out of range!\n");
			return false;
		}
		if (results[column].size() != expected_data.size()) {
			fprintf(stderr, "Row counts do not match!\n");
			return false;
		}
		for(size_t i = 0; i < expected_data.size(); i++) {
			if (expected_data[i] != results[column][i]) {
				fprintf(stderr, "Value does not match: expected \"%s\" but got \"%s\"\n", expected_data[i].c_str(), results[column][i].c_str());
				return false;
			}
		}
		return true;
	}
};

class SQLiteStmtWrapper {
public:
	SQLiteStmtWrapper() :
		stmt(nullptr) {}
	~SQLiteStmtWrapper() {
		if (stmt) {
			sqlite3_finalize(stmt);
		}
	}

	sqlite3_stmt *stmt;
	string error_message;
};

TEST_CASE("Basic sqlite wrapper usage", "[sqlite3wrapper]" ) {
	SQLiteDBWrapper db;

	// open an in-memory db
	REQUIRE(db.Open(":memory:"));

	// standard selection
	REQUIRE(db.Execute("SELECT 42;"));
	REQUIRE(db.CheckColumn(0, {"42"}));

	// simple statements
	REQUIRE(db.Execute("CREATE TABLE test(i INTEGER)"));
	REQUIRE(db.Execute("INSERT INTO test VALUES (1), (2), (3)"));
	REQUIRE(db.Execute("SELECT SUM(t1.i) FROM test t1, test t2, test t3;"));
	REQUIRE(db.CheckColumn(0, {"54"}));

	REQUIRE(db.Execute("DELETE FROM test WHERE i=2"));
	REQUIRE(db.Execute("UPDATE test SET i=i+1"));
	REQUIRE(db.Execute("SELECT * FROM test ORDER BY 1;"));
	REQUIRE(db.CheckColumn(0, {"2", "4"}));

	// test different types
	REQUIRE(db.Execute("SELECT DATE '1992-01-01', 3, 'hello world', TIMESTAMP '1992-01-01 00:00:00';"));
	REQUIRE(db.CheckColumn(0, {"1992-01-01"}));
	REQUIRE(db.CheckColumn(1, {"3"}));
	REQUIRE(db.CheckColumn(2, {"hello world"}));
	REQUIRE(db.CheckColumn(3, {"1992-01-01 00:00:00"}));

	// handle errors
	// syntax error
	REQUIRE(!db.Execute("SELEC 42"));
	// catalog error
	REQUIRE(!db.Execute("SELECT * FROM nonexistant_tbl"));


}
