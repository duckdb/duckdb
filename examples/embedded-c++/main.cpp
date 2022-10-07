#include "duckdb.hpp"

using namespace duckdb;

int main() {
	DuckDB db(nullptr);

	Connection con(db);

	// auto extrv1 = con.Query("INSTALL 'fts';");
	// extrv1->Print();
	// auto extrv2 = con.Query("LOAD 'fts';");
	// extrv2->Print();

	// auto rv_ext = con.Query("select * From duckdb_extensions();");
	// rv_ext->Print();

	auto rv1 = con.Query("CREATE TABLE integers(i1 INTEGER, i2 INET, g Geometry)");
	rv1->Print();
	con.Query("INSERT INTO integers VALUES (1, '127.0.0000.2', 'POINT(100 0)')");
	con.Query("INSERT INTO integers VALUES (2, '127.0.0000.3', 'LINE(1 0)')");
	auto rv2 = con.Query("INSERT INTO integers VALUES (3, '127.0.0000.1', 'POINT(0 1)')");
	rv2->Print();
	con.Query("INSERT INTO integers VALUES (4, '127.0.0000.3', '010100000000000000000000000000000000001440')");
	con.Query("INSERT INTO integers VALUES (5, '127.0.0000.4', '010100000000000000000000000000000000004940')");
	auto result = con.Query("SELECT i1, g::VARCHAR FROM integers");
	result->Print();
}
