#pragma once

#include "duckdb.hpp"

namespace duckdb {

class HTTPFsExtension : public Extension {
public:
	void Load(DuckDB &db) override;
};

} // namespace duckdb
