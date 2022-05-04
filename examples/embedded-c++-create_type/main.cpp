#include "duckdb.hpp"
#include <iostream>

using namespace duckdb;

int main() {
	DuckDB db(nullptr);
	Connection con(db);

	auto createtype_rv = con.Query("CREATE TYPE box (input := 'to_base64', output := 'from_base64')");
	auto t_custom_type_rv = con.Query("CREATE TABLE person_string (name text, current_box box)");
	auto ic_type_rv = con.Query("INSERT INTO person_string VALUES ('Tuyen', 'sadd'::BLOB), ('Nga', encode('happy'))");
	auto sc_type_rv = con.Query("SELECT * FROM person_string ORDER BY name;");
	sc_type_rv->Print();
}