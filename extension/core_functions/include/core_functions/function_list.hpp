//===----------------------------------------------------------------------===//
//                         DuckDB
//
// extension/core_functions/include/core_functions/function_list.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function_list.hpp"

namespace duckdb {

struct CoreFunctionList {
	static const StaticFunctionDefinition *GetFunctionList();
};

} // namespace duckdb
