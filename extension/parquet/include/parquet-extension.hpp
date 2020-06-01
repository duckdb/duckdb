#pragma once

#include "duckdb/main/extension.hpp"

namespace duckdb {

class ParquetExtension : public Extension {
public:
	void Load(DuckDB &db) override;
};

} // namespace duckdb
