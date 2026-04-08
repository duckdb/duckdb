//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_scalar_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"

namespace duckdb {
class ScalarFunction;

struct CreateScalarFunctionInfo : public CreateFunctionInfo {
	DUCKDB_API explicit CreateScalarFunctionInfo(ScalarFunction function);
	DUCKDB_API explicit CreateScalarFunctionInfo(ScalarFunctionSet set);

	ScalarFunctionSet functions;

public:
	DUCKDB_API unique_ptr<CreateInfo> Copy() const override;
	DUCKDB_API unique_ptr<AlterInfo> GetAlterInfo() const override;
};

} // namespace duckdb
