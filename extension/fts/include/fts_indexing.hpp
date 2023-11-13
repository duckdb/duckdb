//===----------------------------------------------------------------------===//
//                         DuckDB
//
// fts_indexing.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/client_context.hpp"

namespace duckdb {

struct FTSIndexing {
	static string DropFTSIndexQuery(ClientContext &context, const FunctionParameters &parameters);
	static string CreateFTSIndexQuery(ClientContext &context, const FunctionParameters &parameters);
};

} // namespace duckdb
