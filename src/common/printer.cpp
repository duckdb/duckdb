#include "duckdb/common/printer.hpp"

#include <stdio.h>

namespace duckdb {

void Printer::Print(string str) {
#ifndef DUCKDB_DISABLE_PRINT
	fprintf(stderr, "%s\n", str.c_str());
#endif
}

} // namespace duckdb
