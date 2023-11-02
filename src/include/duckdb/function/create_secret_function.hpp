//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/create_secret_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/secret.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/parser/parsed_data/pragma_info.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {
class ClientContext;

struct CreateSecretInput {
	//! type
	string type;
	//! mode
	string provider;
	//! (optional) alias provided by user
	string name;
	//! (optional) scope provided by user
	vector<string> scope;
	//! (optional) named parameter map, each create secret function has defined it's own set of these
	named_parameter_map_t named_parameters;
};

//! Execute the create secret function TODO: i guess this needs to return the Auth thingy, then the operator can
//!		put it into the secret manager and call the extension hooks.
typedef shared_ptr<RegisteredSecret> (*create_secret_function_t)(ClientContext &context, CreateSecretInput& input);

class CreateSecretFunction : public SimpleNamedParameterFunction {
public:
	DUCKDB_API CreateSecretFunction (const string &type, const string& mode, create_secret_function_t function);
	DUCKDB_API string ToString() const override;

public:
	create_secret_function_t function;
	named_parameter_type_map_t named_parameters;
};

} // namespace duckdb
