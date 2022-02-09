#include "duckdb.hpp"

using namespace duckdb;

int main() {
	DuckDB db(nullptr);
	Connection con(db);

	auto create_rv = con.Query("CREATE TABLE test (blob_col BLOB NOT NULL, id INTEGER NOT NULL, geom GEOMETRY)");
	auto insert_rv = con.Query("INSERT INTO test VALUES ('AB'::BLOB, 1, 'POINT(0 0)'::GEOMETRY)");
	auto result = con.Query("SELECT * FROM test");
	result->Print();
}