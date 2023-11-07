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
class BaseSecret;

typedef unique_ptr<BaseSecret> (*secret_deserializer_t)(Deserializer& deserializer, BaseSecret base_secret);

//! Secret types describe which secret types are currently registered and how to deserialize them
struct SecretType {
	//! Unique name identifying the secret type
	string name;
	//! The deserialization function for the type
	secret_deserializer_t deserializer;
	//! The default provider of the type
	string default_provider;
};

class SecretManager {
public:
	//! Deserialize the secret. Will look up the deserialized type, then call the deserialize for the registered type.
	unique_ptr<BaseSecret> DeserializeSecret(Deserializer& deserializer);

	//! Registers a secret type
	void RegisterSecretType(SecretType& type);

	//! Register a new Secret in the secret
	void RegisterSecret(shared_ptr<BaseSecret> secret, OnCreateConflict on_conflict);

	//! Get the secret that matches the path best (longest prefix match wins)
	shared_ptr<BaseSecret> GetSecretByPath(string& path, string type);
	//! Get a secret by name
	shared_ptr<BaseSecret> GetSecretByName(string& name);

	//! Get the registered type
	SecretType LookupType(string &type);

	//! Get a vector of all registered secrets
	vector<shared_ptr<BaseSecret>>& AllSecrets();

protected:
	//! The currently registered secrets
	vector<shared_ptr<BaseSecret>> registered_secrets;
	//! The currently registered secret types
	case_insensitive_map_t<SecretType> registered_types;
};

} // namespace duckdb
