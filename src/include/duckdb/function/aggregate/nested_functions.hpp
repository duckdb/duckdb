//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/aggregate/distributive_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {


struct ListBindData : public FunctionData {
	SQLType sql_type;

	ListBindData(SQLType sql_type) : sql_type(sql_type) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<ListBindData>(sql_type);
	}
};

struct ListFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
