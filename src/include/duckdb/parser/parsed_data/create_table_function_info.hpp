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
	explicit CreateTableFunctionInfo(TableFunctionSet set)
	    : CreateFunctionInfo(CatalogType::TABLE_FUNCTION_ENTRY), functions(move(set.functions)) {
		this->name = set.name;
	}
	explicit CreateTableFunctionInfo(TableFunction function) : CreateFunctionInfo(CatalogType::TABLE_FUNCTION_ENTRY) {
		this->name = function.name;
		functions.push_back(move(function));
	}

	//! The table functions
	vector<TableFunction> functions;

public:
	unique_ptr<CreateInfo> Copy() const override {
		TableFunctionSet set(name);
		set.functions = functions;
		auto result = make_unique<CreateTableFunctionInfo>(move(set));
		CopyProperties(*result);
		return move(result);
	}
};

} // namespace duckdb
