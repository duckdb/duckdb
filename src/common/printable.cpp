
#include <stdio.h>

#include "common/printable.hpp"

using namespace duckdb;

void Printable::Print() { fprintf(stderr, "%s\n", ToString().c_str()); }
