//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/secret/secret_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/main/secret/secret.hpp"

namespace duckdb {
struct CreateSecretInfo;
struct BoundStatement;
class CreateSecretStatement;

//! Registered secret is a wrapper around a secret containing metadata from the secret manager
struct RegisteredSecret {
public:
	RegisteredSecret(shared_ptr<const BaseSecret> secret) : secret(secret){};
	//! Whether this secret is persistent
	bool persistent;
	//! Metadata for user on how the secret is stored. (DuckSecretManager will set this to the path)
	string storage_mode;
	//! The secret pointer
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
	//! Get the registered type
	DUCKDB_API virtual SecretType LookupType(const string &type) = 0;
	//! Registers a create secret function
	DUCKDB_API virtual void RegisterSecretFunction(CreateSecretFunction function, OnCreateConflict on_conflict) = 0;
	//! Register a Secret directly
	DUCKDB_API virtual void RegisterSecret(shared_ptr<const BaseSecret> secret, OnCreateConflict on_conflict, SecretPersistMode persist_mode) = 0;
	//! Create & Register a secret by looking up the function
	DUCKDB_API virtual void CreateSecret(ClientContext &context, const CreateSecretInfo &input) = 0;
	//! Binds a create secret statement
	DUCKDB_API virtual BoundStatement BindCreateSecret(CreateSecretStatement &stmt) = 0;
	//! Get the secret whose scope best matches the path.
	DUCKDB_API virtual RegisteredSecret GetSecretByPath(const string &path, const string &type) = 0;
	//! Get a secret by name
	DUCKDB_API virtual RegisteredSecret GetSecretByName(const string &name) = 0;
	//! Drop a secret by name
	DUCKDB_API virtual void DropSecretByName(const string &name, bool missing_ok) = 0;
	//! Get a vector of all registered secrets
	DUCKDB_API virtual vector<RegisteredSecret> AllSecrets() = 0;
};

//! The debug secret manager demonstrates how the Base Secret Manager can be extended
class DebugSecretManager : public SecretManager {
public:
	virtual ~DebugSecretManager() override = default;
	DebugSecretManager(unique_ptr<SecretManager> secret_manager) : base_secret_manager(std::move(secret_manager)){};

	DUCKDB_API virtual unique_ptr<BaseSecret> DeserializeSecret(Deserializer &deserializer) override;
	DUCKDB_API virtual void RegisterSecretType(SecretType &type) override;
	DUCKDB_API virtual void RegisterSecretFunction(CreateSecretFunction function, OnCreateConflict on_conflict) override;
	DUCKDB_API virtual void RegisterSecret(shared_ptr<const BaseSecret> secret, OnCreateConflict on_conflict, SecretPersistMode persist_mode) override;
	DUCKDB_API virtual void CreateSecret(ClientContext &context, const CreateSecretInfo &info) override;
	DUCKDB_API virtual BoundStatement BindCreateSecret(CreateSecretStatement &stmt) override;
	DUCKDB_API virtual RegisteredSecret GetSecretByPath(const string &path, const string &type) override;
	DUCKDB_API virtual RegisteredSecret GetSecretByName(const string &name) override;
	DUCKDB_API virtual void DropSecretByName(const string &name, bool missing_ok) override;
	DUCKDB_API virtual SecretType LookupType(const string &type) override;
	DUCKDB_API virtual vector<RegisteredSecret> AllSecrets() override;

protected:
	unique_ptr<SecretManager> base_secret_manager;
};

} // namespace duckdb
