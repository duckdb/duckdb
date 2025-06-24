//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/secret/default_secrets.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {
class DatabaseInstance;
class ClientContext;
class BaseSecret;
struct CreateSecretInput;
class SecretManager;
struct SecretType;
class CreateSecretFunction;

struct CreateHTTPSecretFunctions {
public:
	//! Get the default secret types
	static vector<SecretType> GetDefaultSecretTypes();
	//! Get the default secret functions
	static vector<CreateSecretFunction> GetDefaultSecretFunctions();

protected:
	//! HTTP secret CONFIG provider
	static unique_ptr<BaseSecret> CreateHTTPSecretFromConfig(ClientContext &context, CreateSecretInput &input);
	//! HTTP secret ENV provider
	static unique_ptr<BaseSecret> CreateHTTPSecretFromEnv(ClientContext &context, CreateSecretInput &input);
};

} // namespace duckdb
