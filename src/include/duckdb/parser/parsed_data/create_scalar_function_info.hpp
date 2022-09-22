//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_scalar_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct CreateScalarFunctionInfo : public CreateFunctionInfo {
	explicit CreateScalarFunctionInfo(ScalarFunction function);
	explicit CreateScalarFunctionInfo(ScalarFunctionSet set);

	ScalarFunctionSet functions;

public:
	unique_ptr<CreateInfo> Copy() const override;
	unique_ptr<AlterInfo> GetAlterInfo() const override;
};

} // namespace duckdb
