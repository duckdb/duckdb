#include <iostream>
#include <string>

#include "duckdb/common/platform.hpp"

int main() {
	std::cout << duckdb::DuckDBPlatform();
	return 0;
}
