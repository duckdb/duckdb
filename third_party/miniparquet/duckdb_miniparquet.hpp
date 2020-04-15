#pragma once

#include "duckdb.hpp"

namespace duckdb {
class Parquet {
public:
	static void Init(DuckDB& db);
};

}
