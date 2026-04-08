//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_copy_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/winapi.hpp"

namespace duckdb {

struct CreateCopyFunctionInfo : public CreateInfo {
	DUCKDB_API explicit CreateCopyFunctionInfo(CopyFunction function);

	//! Function name
	string name;
	//! The table function
	CopyFunction function;

public:
	unique_ptr<CreateInfo> Copy() const override;
};

} // namespace duckdb
