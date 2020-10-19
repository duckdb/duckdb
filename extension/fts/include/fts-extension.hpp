#pragma once

#include "duckdb.hpp"

namespace duckdb {

class FTSExtension : public Extension {
public:
	void Load(DuckDB &db) override;
};

} // namespace duckdb