//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_pragma_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"

namespace duckdb {
class PragmaFunction;

struct CreatePragmaFunctionInfo : public CreateFunctionInfo {
	DUCKDB_API explicit CreatePragmaFunctionInfo(PragmaFunction function);
	DUCKDB_API CreatePragmaFunctionInfo(string name, PragmaFunctionSet functions);

	PragmaFunctionSet functions;

public:
	DUCKDB_API unique_ptr<CreateInfo> Copy() const override;
};

} // namespace duckdb
