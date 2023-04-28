#include "duckdb.hpp"

#include <iostream>

extern "C" {

int main() {
	std::cout << "Hello from WASM" << std::endl;
}

int32_t HelloWasm() {
	duckdb::DBConfig config;
	config.options.maximum_threads = 1;
	duckdb::DuckDB db(nullptr, &config);
	duckdb::Connection con(db);
	auto result = con.Query("CREATE TABLE sometable AS SELECT x FROM generate_series(1,10000) AS a(x)");
	if (result->HasError()) {
		std::cerr << result->GetError() << std::endl;
		return -1;
	}
	result = con.Query("SELECT sum(x) FROM sometable");
	if (result->HasError()) {
		std::cerr << result->GetError() << std::endl;
		return -1;
	}
	return result->GetValue(0, 0).GetValue<int64_t>();
}
}
