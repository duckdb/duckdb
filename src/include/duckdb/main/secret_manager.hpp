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
class RegisteredSecret;

typedef unique_ptr<RegisteredSecret> (*secret_deserializer_t)(Deserializer& deserializer);

//! Secret types describe which secret types are currently registered and how to deserialize them
struct SecretType {
	//! Unique name identifying the secret type
	string name;
	//! The deserialization function for the type
	secret_deserializer_t deserializer;

	// TODO add default provider
};

class SecretManager {
public:
	//! Deserialize the secret. Will look up the deserialized type, then call the deserialize for the registered type.
	static unique_ptr<RegisteredSecret> DeserializeSecret(Deserializer& deserializer);
	//! Serialize the secret. Calls the virtual serialize function of the secret;
	static void SerializeSecret(RegisteredSecret& secret, Serializer& serializer);

	//! Registers a secret type
	void RegisterSecretType(SecretType& type);

	//! Register a new Secret in the secret
	void RegisterSecret(shared_ptr<RegisteredSecret> secret, OnCreateConflict on_conflict);

	//! Get the secret that matches the path best (longest prefix match wins)
	shared_ptr<RegisteredSecret> GetSecretByPath(string& path, string type);
	//! Get a secret by name
	shared_ptr<RegisteredSecret> GetSecretByName(string& name);

	//! Get the registered type
	SecretType LookupType(string &type);

	//! Get a vector of all registered secrets
	vector<shared_ptr<RegisteredSecret>>& AllSecrets();

protected:
	//! The currently registered secrets
	vector<shared_ptr<RegisteredSecret>> registered_secrets;
	//! The currently registered secret types
	case_insensitive_map_t<SecretType> registered_types;
};

} // namespace duckdb
