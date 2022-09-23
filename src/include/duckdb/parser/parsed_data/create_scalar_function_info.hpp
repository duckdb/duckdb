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
	DUCKDB_API explicit CreateScalarFunctionInfo(ScalarFunction function);
	DUCKDB_API explicit CreateScalarFunctionInfo(ScalarFunctionSet set);

	ScalarFunctionSet functions;

public:
	DUCKDB_API unique_ptr<CreateInfo> Copy() const override;
	DUCKDB_API unique_ptr<AlterInfo> GetAlterInfo() const override;
};

} // namespace duckdb
