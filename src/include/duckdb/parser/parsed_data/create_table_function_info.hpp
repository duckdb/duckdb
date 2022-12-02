//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_table_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct CreateTableFunctionInfo : public CreateFunctionInfo {
	explicit CreateTableFunctionInfo(TableFunction function);
	explicit CreateTableFunctionInfo(TableFunctionSet set);

	//! The table functions
	TableFunctionSet functions;

public:
	unique_ptr<CreateInfo> Copy() const;
};

} // namespace duckdb
