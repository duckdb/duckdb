#include "duckdb/common/platform.h"
#include <iostream>

int main() {
	std::cout << duckdb::DuckDBPlatform() << "\n";
	return 0;
}
