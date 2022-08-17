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
	explicit CreateTableFunctionInfo(TableFunction function)
	    : CreateFunctionInfo(CatalogType::TABLE_FUNCTION_ENTRY), functions(function.name) {
		name = function.name;
		functions.AddFunction(move(function));
	}
	explicit CreateTableFunctionInfo(TableFunctionSet set)
	    : CreateFunctionInfo(CatalogType::TABLE_FUNCTION_ENTRY), functions(move(set)) {
		name = functions.name;
		for (auto &func : functions.functions) {
			func.name = functions.name;
		}
	}

	//! The table functions
	TableFunctionSet functions;

public:
	unique_ptr<CreateInfo> Copy() const override {
		TableFunctionSet set(name);
		set.functions = functions.functions;
		auto result = make_unique<CreateTableFunctionInfo>(move(set));
		CopyProperties(*result);
		return move(result);
	}
};

} // namespace duckdb
