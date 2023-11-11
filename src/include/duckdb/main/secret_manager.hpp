//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/secret_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/main/secret.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"

namespace duckdb {
class ClientContext;
class BaseSecret;

typedef unique_ptr<BaseSecret> (*secret_deserializer_t)(Deserializer &deserializer, BaseSecret base_secret);

//! Secret types describe which secret types are currently registered and how to deserialize them
struct SecretType {
	//! Unique name identifying the secret type
	string name;
	//! The deserialization function for the type
	secret_deserializer_t deserializer;
	//! The default provider of the type
	string default_provider;
};

//! Registered secret is a Wrapper around a secret containing metadata from the secret manager
struct RegisteredSecret {
public:
	RegisteredSecret(shared_ptr<const BaseSecret> secret) : secret(secret){};

	//! Whether this secret will survive restarts
	bool persistent;
	//! A string to tell users how the secret is stored. When persistent secrets are implemented, this will communicate
	//! how the secrets are to be stored.
	string storage_mode;

	//! The secret pointer: note it is a const pointer, since secrets can not be updated
	shared_ptr<const BaseSecret> secret;
};

//! Base secret manager class
class SecretManager {
	friend struct RegisteredSecret;

public:
	virtual ~SecretManager() = default;

	//! Deserialize the secret. Will look up the deserialized type, then call the deserialize for the registered type.
	DUCKDB_API virtual unique_ptr<BaseSecret> DeserializeSecret(Deserializer &deserializer) = 0;
	//! Registers a secret type
	DUCKDB_API virtual void RegisterSecretType(SecretType &type) = 0;
	//! Register a new Secret in the secret
	DUCKDB_API virtual void RegisterSecret(shared_ptr<const BaseSecret> secret, OnCreateConflict on_conflict, SecretPersistMode persist_mode) = 0;
	//! Get the secret that matches the scope best. ( Default behaviour is to match longest matching prefix )
	DUCKDB_API virtual RegisteredSecret GetSecretByPath(const string &path, const string &type) = 0;
	//! Get a secret by name
	DUCKDB_API virtual RegisteredSecret GetSecretByName(const string &name) = 0;
	//! Drop a secret by name
	DUCKDB_API virtual void DropSecretByName(const string &name, bool missing_ok) = 0;
	//! Get the registered type
	DUCKDB_API virtual SecretType LookupType(const string &type) = 0;
	//! Get a vector of all registered secrets
	DUCKDB_API virtual vector<RegisteredSecret> &AllSecrets() = 0;
};

//! The main DuckDB secret manager
class DuckSecretManager : public SecretManager {
	friend struct RegisteredSecret;

public:
	virtual ~DuckSecretManager() override = default;

	//! Deserialize the secret. Will look up the deserialized type, then call the deserialize for the registered type.
	DUCKDB_API virtual unique_ptr<BaseSecret> DeserializeSecret(Deserializer &deserializer) override;
	//! Registers a secret type
	DUCKDB_API virtual void RegisterSecretType(SecretType &type) override;
	//! Register a new Secret in the secret
	DUCKDB_API virtual void RegisterSecret(shared_ptr<const BaseSecret> secret, OnCreateConflict on_conflict, SecretPersistMode persist_mode) override;
	//! Get the secret that matches the scope best. ( Default behaviour is to match longest matching prefix )
	DUCKDB_API virtual RegisteredSecret GetSecretByPath(const string &path, const string &type) override;
	//! Get a secret by name
	DUCKDB_API virtual RegisteredSecret GetSecretByName(const string &name) override;
	//! Drop a secret by name
	DUCKDB_API virtual void DropSecretByName(const string &name, bool missing_ok) override;
	//! Get the registered type
	DUCKDB_API virtual SecretType LookupType(const string &type) override;
	//! Get a vector of all registered secrets
	DUCKDB_API virtual vector<RegisteredSecret> &AllSecrets() override;

private:
	//! Get the registered type
	SecretType LookupTypeInternal(const string &type);

	//! Main lock
	mutex lock;
	//! The currently registered secrets
	vector<RegisteredSecret> registered_secrets;
	//! The currently registered secret types
	case_insensitive_map_t<SecretType> registered_types;
};

class DebugSecretManager : public SecretManager {
public:
	virtual ~DebugSecretManager() override = default;

	//! Example secret manager will take the base secret manager
	DebugSecretManager(unique_ptr<SecretManager> secret_manager) : base_secret_manager(std::move(secret_manager)){};
	//! Deserialize the secret. Will look up the deserialized type, then call the deserialize for the registered type.
	DUCKDB_API virtual unique_ptr<BaseSecret> DeserializeSecret(Deserializer &deserializer) override;
	//! Registers a secret type
	DUCKDB_API virtual void RegisterSecretType(SecretType &type) override;
	//! Register a new Secret in the secret
	DUCKDB_API virtual void RegisterSecret(shared_ptr<const BaseSecret> secret, OnCreateConflict on_conflict, SecretPersistMode persist_mode) override;
	//! Get the secret that matches the scope best. ( Default behaviour is to match longest matching prefix )
	DUCKDB_API virtual RegisteredSecret GetSecretByPath(const string &path, const string &type) override;
	//! Get a secret by name
	DUCKDB_API virtual RegisteredSecret GetSecretByName(const string &name) override;
	//! Drop a secret by name
	DUCKDB_API virtual void DropSecretByName(const string &name, bool missing_ok) override;
	//! Get the registered type
	DUCKDB_API virtual SecretType LookupType(const string &type) override;
	//! Get a vector of all registered secrets
	DUCKDB_API virtual vector<RegisteredSecret> &AllSecrets() override;

protected:
	unique_ptr<SecretManager> base_secret_manager;
};

} // namespace duckdb
