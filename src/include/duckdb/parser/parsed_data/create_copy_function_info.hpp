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

#include "duckdb/common/identifier.hpp"
namespace duckdb {

struct CreateCopyFunctionInfo : public CreateInfo {
	DUCKDB_API explicit CreateCopyFunctionInfo(CopyFunction function);

	//! Function name
	const Identifier &GetCopyFunctionName() const {
		return qualified_name.Name();
	}
	void SetCopyFunctionName(Identifier name) {
		qualified_name = qualified_name.WithName(std::move(name));
	}
	//! The table function
	CopyFunction function;

public:
	unique_ptr<CreateInfo> Copy() const override;
};

} // namespace duckdb
