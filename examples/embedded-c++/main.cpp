#include "duckdb.hpp"
#include <iostream>

using namespace duckdb;

int main() {
	DuckDB db(nullptr);
	Connection con(db);
	auto create_result = con.Query("CREATE TABLE integers(i INTEGER, j INTEGER)");
	if (!create_result || create_result->HasError()) {
		std::cerr << "Failed to create table: " << create_result->GetError() << std::endl;
		return 1;
	}
	auto insert_result = con.Query("INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL)");
	if (!insert_result || insert_result->HasError()) {
		std::cerr << "Failed to insert rows: " << insert_result->GetError() << std::endl;
		return 1;
	}
	auto result = con.Query("SELECT * FROM integers");
	if (!result || result->HasError()) {
		std::cerr << "Failed to query table: " << result->GetError() << std::endl;
		return 1;
	}
	result->Print();
	return 0;
}
