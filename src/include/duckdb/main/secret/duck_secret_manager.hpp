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
struct DBConfig;

//! The Main secret manager implementation for DuckDB. The secret manager can handle both temporary and permanent
//! secrets
class DuckSecretManager : public SecretManager {
	friend struct SecretEntry;

public:
	explicit DuckSecretManager() = default;
	virtual ~DuckSecretManager() override = default;

	//! SecretManager API
	DUCKDB_API void Initialize(DatabaseInstance &db) override;
	DUCKDB_API unique_ptr<BaseSecret> DeserializeSecret(CatalogTransaction transaction,
	                                                    Deserializer &deserializer) override;
	DUCKDB_API void RegisterSecretType(SecretType &type) override;
	DUCKDB_API SecretType LookupType(const string &type) override;
	DUCKDB_API void RegisterSecretFunction(CreateSecretFunction function, OnCreateConflict on_conflict) override;
	DUCKDB_API optional_ptr<SecretEntry> RegisterSecret(CatalogTransaction transaction,
	                                                    unique_ptr<const BaseSecret> secret,
	                                                    OnCreateConflict on_conflict,
	                                                    SecretPersistMode persist_mode) override;
	DUCKDB_API optional_ptr<SecretEntry> CreateSecret(ClientContext &context, const CreateSecretInfo &info) override;
	DUCKDB_API BoundStatement BindCreateSecret(CreateSecretStatement &stmt) override;
	DUCKDB_API optional_ptr<SecretEntry> GetSecretByPath(CatalogTransaction transaction, const string &path,
	                                                     const string &type) override;
	DUCKDB_API optional_ptr<SecretEntry> GetSecretByName(CatalogTransaction transaction, const string &name) override;
	DUCKDB_API void DropSecretByName(CatalogTransaction transaction, const string &name, bool missing_ok) override;
	DUCKDB_API vector<SecretEntry *> AllSecrets(CatalogTransaction transaction) override;
	DUCKDB_API bool AllowConfigChanges() override;

private:
	//! Deserialize a secret
	unique_ptr<BaseSecret> DeserializeSecretInternal(Deserializer &deserializer);
	//! Lookup a SecretType
	SecretType LookupTypeInternal(const string &type);
	//! Lookup a CreateSecretFunction
	CreateSecretFunction *LookupFunctionInternal(const string &type, const string &provider);
	//! Register a new Secret
	optional_ptr<SecretEntry> RegisterSecretInternal(CatalogTransaction transaction,
	                                                 unique_ptr<const BaseSecret> secret, OnCreateConflict on_conflict,
	                                                 SecretPersistMode persist_mode);

	//! Write a secret to the FileSystem
	void WriteSecretToFile(CatalogTransaction transaction, const BaseSecret &secret);

	//! Lazily preloads the permanent secrets f
	void PreloadPermanentSecrets(CatalogTransaction transaction);

	//! Fully loads ALL lazily preloaded permanent secrets that have not yet been preloaded
	void LoadPreloadedSecrets(CatalogTransaction transaction);
	//! Fully load a lazily preloaded permanent secret by path
	void LoadSecret(CatalogTransaction transaction, const string &path, SecretPersistMode persist_mode);
	//! Fully load a lazily preloaded permanent secret by name
	void LoadSecretFromPreloaded(CatalogTransaction transaction, const string &name);

	//! Checks if the secret_directory changed, if so this reloads all permanent secrets (lazily)
	void SyncPermanentSecrets(CatalogTransaction transaction, bool force = false);
	//! Return secret directory
	string GetSecretDirectory(DBConfig &config);

	//! Secrets
	unique_ptr<CatalogSet> registered_secrets;
	// Types
	case_insensitive_map_t<SecretType> registered_types;
	// Functions
	case_insensitive_map_t<CreateSecretFunctionSet> create_secret_functions;

	//! Maps secret name -> permanent secret file path. Permanent secrets are loaded lazily into this map. Then loaded
	//! into registered_secrets when they are needed
	case_insensitive_map_t<string> permanent_secret_files;
	//! When fs is initialized changing the secret path is prohibited
	bool initialized_fs = false;

	//! Secret manager needs to be thread-safe
	mutex lock;
};

} // namespace duckdb
