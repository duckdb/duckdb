#include "duckdb/common/platform.h"
#include <iostream>

int main() {
	std::cout << duckdb::DuckDBPlatform();
	return 0;
}
