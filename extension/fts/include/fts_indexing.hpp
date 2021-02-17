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

string drop_fts_index_query(ClientContext &context, const FunctionParameters &parameters);
string create_fts_index_query(ClientContext &context, const FunctionParameters &parameters);

} // namespace duckdb
