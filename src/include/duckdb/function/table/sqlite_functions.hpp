//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/sqlite_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

FunctionData *pragma_table_info_init(ClientContext &);
void pragma_table_info(ClientContext &, DataChunk &input, DataChunk &output, FunctionData *dataptr);

FunctionData *sqlite_master_init(ClientContext &);
void sqlite_master(ClientContext &, DataChunk &input, DataChunk &output, FunctionData *dataptr);

} // namespace duckdb
