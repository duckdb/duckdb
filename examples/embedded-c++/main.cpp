#include "duckdb.hpp"

using namespace duckdb;

int main() {
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	con.Query("CREATE TABLE integers(i INTEGER)");
	con.Query("INSERT INTO integers VALUES (3)");
	auto result = con.Query("SELECT * FROM integers");
	result->Print();
}
