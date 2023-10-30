//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/create_secret_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function.hpp"
#include "duckdb/parser/parsed_data/pragma_info.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
class ClientContext;

//! Execute the create secret function TODO: i guess this needs to return the Auth thingy, then the operator can
//!		put it into the credential manager and call the extension hooks.
typedef void (*create_secret_function_t)(ClientContext &context, named_parameter_map_t named_parameters);

class CreateSecretFunction : public SimpleNamedParameterFunction {
public:
	DUCKDB_API CreateSecretFunction (const string &type, const string& mode, create_secret_function_t function);
	DUCKDB_API string ToString() const override;

public:
	create_secret_function_t function;
	named_parameter_type_map_t named_parameters;
};

} // namespace duckdb
