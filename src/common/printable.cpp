#include "common/printable.hpp"

#include <stdio.h>

using namespace duckdb;

void Printable::Print() {
	fprintf(stderr, "%s\n", ToString().c_str());
}
