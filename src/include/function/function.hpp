//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// function/function.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"

#include "parser/column_definition.hpp"

namespace duckdb {
class Catalog;
class ClientContext;
class Transaction;

//! Type used for initialization function
typedef void (*table_function_init_t)(ClientContext &, void **dataptr);
//! Type used for table-returning function
typedef void (*table_function_t)(ClientContext &, DataChunk &input,
                                 DataChunk &output, void **dataptr);
//! Type used for final (cleanup) function
typedef void (*table_function_final_t)(ClientContext &, void **dataptr);

class BuiltinFunctions {
  public:
	//! Initialize a catalog with all built-in functions
	static void Initialize(Transaction &transaction, Catalog &catalog);
};

} // namespace duckdb
