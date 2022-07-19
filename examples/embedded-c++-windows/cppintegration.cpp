/**
 * =====================================
 * Simple DuckDb C++ integration Test
 * =====================================
 */

#include "duckdb.hpp"

#include <iostream>

using namespace duckdb;

int main(int argc, char *argv[]) {
	DuckDB db(nullptr);
	Connection con(db);
	auto result = con.Query("SELECT 42");

	// Basic create table and insert
	con.Query("CREATE TABLE people(id INTEGER, name VARCHAR)");
	con.Query("CREATE TABLE test");
	con.Query("INSERT INTO people VALUES (0,'Mark'), (1, 'Hannes')");

	// Update data
	auto prepared = con.Prepare("UPDATE people SET name = $1 WHERE id = $2");
	auto prep = prepared->Execute("DuckDb", 2);

	// Delete data
	auto resultDelete = con.Query("DELETE FROM people WHERE id = 2");

	// Read data
	auto resultSelect = con.Query("SELECT * FROM people");

	return 0;
}
