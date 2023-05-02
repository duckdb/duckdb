#pragma once

#include "duckdb.hpp"

namespace duckdb {

class HTTPFsExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
};

struct CacheRemoteFile {
	static void CacheRemoteFileFunction(duckdb::DataChunk &args, duckdb::ExpressionState &state,
	                                    duckdb::Vector &result);
};

struct DeleteCachedFile {
	static void DeleteCachedFileFunction(duckdb::DataChunk &args, duckdb::ExpressionState &state,
	                                     duckdb::Vector &result);
};

} // namespace duckdb
