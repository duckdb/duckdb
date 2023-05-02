#pragma once

#include "duckdb.hpp"

namespace duckdb {

class HTTPFsExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
};

struct RemoteFileGeneric {
	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names);
};

struct CacheRemoteFile : RemoteFileGeneric {
	static void Function(ClientContext &context, TableFunctionInput &data, DataChunk &output);
};

struct DeleteCachedFile : RemoteFileGeneric {
	static void Function(ClientContext &context, TableFunctionInput &data, DataChunk &output);
};

} // namespace duckdb
