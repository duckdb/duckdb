#pragma once

#include "duckdb_extension.h"

void AddNumbersTogether(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output);
