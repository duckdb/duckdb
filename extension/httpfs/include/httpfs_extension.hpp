#pragma once

#include "duckdb.hpp"

namespace duckdb {

class HttpfsExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
};

} // namespace duckdb
