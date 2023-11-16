//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/secret/duck_secret_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {
class SecretManager;

//! The Main secret manager implementation for DuckDB. The secret manager can handle both temporary and permanent
//! secrets
class DuckSecretManager : public SecretManager {
	friend struct RegisteredSecret;

public:
	explicit DuckSecretManager(DatabaseInstance &instance);
	virtual ~DuckSecretManager() override = default;

	//! SecretManager API
	DUCKDB_API virtual unique_ptr<BaseSecret> DeserializeSecret(Deserializer &deserializer) override;
	DUCKDB_API virtual void RegisterSecretType(SecretType &type) override;
	DUCKDB_API virtual SecretType LookupType(const string &type) override;
	DUCKDB_API virtual void RegisterSecretFunction(CreateSecretFunction function,
	                                               OnCreateConflict on_conflict) override;
	DUCKDB_API virtual void RegisterSecret(shared_ptr<const BaseSecret> secret, OnCreateConflict on_conflict,
	                                       SecretPersistMode persist_mode) override;
	DUCKDB_API virtual void CreateSecret(ClientContext &context, const CreateSecretInfo &info) override;
	DUCKDB_API virtual BoundStatement BindCreateSecret(CreateSecretStatement &stmt) override;
	DUCKDB_API virtual RegisteredSecret GetSecretByPath(const string &path, const string &type) override;
	DUCKDB_API virtual RegisteredSecret GetSecretByName(const string &name) override;
	DUCKDB_API virtual void DropSecretByName(const string &name, bool missing_ok) override;

	//! Get a vector of all registered secrets
	DUCKDB_API virtual vector<RegisteredSecret> AllSecrets() override;

private:
	//! Deserialize a secret
	unique_ptr<BaseSecret> DeserializeSecretInternal(Deserializer &deserializer);
	//! Lookup a SecretType
	SecretType LookupTypeInternal(const string &type);
	//! Lookup a CreateSecretFunction
	CreateSecretFunction *LookupFunctionInternal(const string &type, const string &provider);
	//! Register a new Secret
	void RegisterSecretInternal(shared_ptr<const BaseSecret> secret, OnCreateConflict on_conflict,
	                            SecretPersistMode persist_mode);

	//! Write a secret to the FileSystem
	void WriteSecretToFile(const BaseSecret &secret);

	//! Lazily preloads the permanent secrets f
	void PreloadPermanentSecrets();

	//! Fully loads ALL lazily preloaded permanent secrets that have not yet been preloaded
	void LoadPreloadedSecrets();
	//! Fully load a lazily preloaded permanent secret by path
	void LoadSecret(const string &path, SecretPersistMode persist_mode);
	//! Fully load a lazily preloaded permanent secret by name
	void LoadSecretFromPreloaded(const string &name);

	//! Checks if the secret_directory changed, if so this reloads all permanent secrets (lazily)
	void SyncPermanentSecrets(bool force = false);

	//! Return secret directory
	string GetSecretDirectory();

	//! Current SecretTypes and CreatSecretFunctions
	case_insensitive_map_t<SecretType> registered_types;
	case_insensitive_map_t<CreateSecretFunctionSet> create_secret_functions;

	//! The currently loaded secrets: can contain both temporary and permanent secrets.
	vector<RegisteredSecret> registered_secrets;
	//! Maps secret name -> permanent secret file path. Permanent secrets are loaded lazily into this map. Then loaded
	//! into registered_secrets when they are needed
	case_insensitive_map_t<string> permanent_secret_files;
	//! The permanent secret directory is scanned once. When the secret path changes in DuckDB the secret manager will
	//! need to reload the permanent secrets;
	string last_secret_directory;
	//! The secret manager requires access to the DatabaseInstance for the FileSystem and DBConfig
	DatabaseInstance &db_instance;
	//! Secret manager needs to be thread-safe
	mutex lock;
};

} // namespace duckdb
