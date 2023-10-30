//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_create_secret_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct CreateSecretFunctionInfo : public CreateFunctionInfo {
	DUCKDB_API explicit CreateSecretFunctionInfo(CreateSecretFunction function);
	DUCKDB_API CreateSecretFunctionInfo(string name, CreateSecretFunctionSet functions_);

	CreateSecretFunctionSet functions;

public:
	DUCKDB_API unique_ptr<CreateInfo> Copy() const override;
};

} // namespace duckdb
