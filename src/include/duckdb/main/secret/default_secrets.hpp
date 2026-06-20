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

//! The 'extension_repository' secret pins the trust anchor (signing key) for a self-hosted extension
//! repository. The signing key is a public key, so nothing here is sensitive/redacted.
struct CreateExtensionRepositorySecretFunctions {
public:
	//! Get the extension_repository secret type
	static vector<SecretType> GetDefaultSecretTypes();
	//! Get the extension_repository secret functions
	static vector<CreateSecretFunction> GetDefaultSecretFunctions();

protected:
	//! extension_repository secret CONFIG provider
	static unique_ptr<BaseSecret> CreateExtensionRepositorySecretFromConfig(ClientContext &context,
	                                                                        CreateSecretInput &input);
};

} // namespace duckdb
