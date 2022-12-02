//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_pragma_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct CreatePragmaFunctionInfo : public CreateFunctionInfo {
	explicit CreatePragmaFunctionInfo(PragmaFunction function);
	CreatePragmaFunctionInfo(string name, PragmaFunctionSet functions_);

	PragmaFunctionSet functions;

public:
	unique_ptr<CreateInfo> Copy() const override;
};

} // namespace duckdb
