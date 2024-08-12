#include "duckdb/common/platform.hpp"
#include <iostream>

int main() {
	std::cout << duckdb::DuckDBPlatform();
	return 0;
}
