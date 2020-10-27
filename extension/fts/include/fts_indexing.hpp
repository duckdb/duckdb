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

string pragma_drop_fts_index_query(ClientContext &context, vector<Value> parameters);
string pragma_create_fts_index_query(ClientContext &context, vector<Value> parameters);

} // namespace duckdb
