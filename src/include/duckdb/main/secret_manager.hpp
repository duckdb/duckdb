//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/secret_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/secret.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"

namespace duckdb {
class ClientContext;

class SecretManager {
public:
	//! Register a new Secret in the secret
	void RegisterSecret(shared_ptr<RegisteredSecret> secret, OnCreateConflict on_conflict);

	//! Get the secret that matches the path best (longest prefix match wins)
	shared_ptr<RegisteredSecret> GetSecretByPath(string& path, string type);
	//! Get a secret by name
	shared_ptr<RegisteredSecret> GetSecretByName(string& name);

	//! Get a vector of all registered secrets
	vector<shared_ptr<RegisteredSecret>>& AllSecrets();

protected:
	vector<shared_ptr<RegisteredSecret>> registered_secrets;
};

} // namespace duckdb
