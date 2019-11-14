#include "duckdb/common/printer.hpp"

#include <stdio.h>

using namespace duckdb;

void Printer::Print(string str) {
	fprintf(stderr, "%s\n", str.c_str());
}
