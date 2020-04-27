#pragma once

namespace duckdb {
class DuckDB;
class Parquet {
public:
	static void Init(DuckDB& db);
};

}
