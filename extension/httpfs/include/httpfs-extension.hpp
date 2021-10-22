#pragma once

#include "duckdb.hpp"

namespace duckdb {

class HTTPFsExtension : public Extension {
public:
	void Load(DuckDB &db) override;
};

} // namespace duckdb

extern "C" {
// TODO use DUCKDB_EXTENSION_API here
void httpfs_init(duckdb::DatabaseInstance &db);

const char *httpfs_version();
}