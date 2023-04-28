#pragma once

#include "duckdb.hpp"

namespace duckdb {

class HTTPFsExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
};

struct RegisterCache {
	static void RegisterFunction(duckdb::Connection &conn, duckdb::Catalog &catalog);
};

struct UnregisterCache {
	static void RegisterFunction(duckdb::Connection &conn, duckdb::Catalog &catalog);
};

} // namespace duckdb
