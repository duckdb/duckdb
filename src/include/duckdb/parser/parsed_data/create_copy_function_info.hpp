//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_copy_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/function/copy_function.hpp"

namespace duckdb {

struct CreateCopyFunctionInfo : public CreateInfo {
	explicit CreateCopyFunctionInfo(CopyFunction function)
	    : CreateInfo(CatalogType::COPY_FUNCTION_ENTRY), function(function) {
		this->name = function.name;
	}

	//! Function name
	string name;
	//! The table function
	CopyFunction function;

public:
	unique_ptr<CreateInfo> Copy() const override {
		auto result = make_unique<CreateCopyFunctionInfo>(function);
		CopyProperties(*result);
		return move(result);
	}
};

} // namespace duckdb
