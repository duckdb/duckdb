#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include "catch.hpp"
#include "test_helpers.hpp"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
	unique_ptr<duckdb::QueryResult> result;
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);

	std::string input(reinterpret_cast<const char*>(data), size);
	try {
		auto statements = con.Query(input);
	} catch (const std::exception& e) {
		return 1;
	}
	return 1;
}
