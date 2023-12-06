//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/secret/duck_secret_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/default/default_generator.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {
class SecretManager;
struct DBConfig;
class SchemaCatalogEntry;

//! The Main secret manager implementation for DuckDB. Can handle both temporary and permanent secrets
class DuckSecretManager : public SecretManager {
	friend struct SecretEntry;

public:
	explicit DuckSecretManager() = default;
	virtual ~DuckSecretManager() override = default;

	//! SecretManager API
	DUCKDB_API void Initialize(DatabaseInstance &db) override;
	DUCKDB_API unique_ptr<BaseSecret> DeserializeSecret(CatalogTransaction transaction,
	                                                    Deserializer &deserializer) override;
	DUCKDB_API void RegisterSecretType(CatalogTransaction transaction, SecretType &type) override;
	DUCKDB_API SecretType LookupType(CatalogTransaction transaction, const string &type) override;
	DUCKDB_API void RegisterSecretFunction(CatalogTransaction transaction, CreateSecretFunction function,
	                                       OnCreateConflict on_conflict) override;
	DUCKDB_API optional_ptr<SecretEntry> RegisterSecret(CatalogTransaction transaction,
	                                                    unique_ptr<const BaseSecret> secrsecretet,
	                                                    OnCreateConflict on_conflict,
	                                                    SecretPersistMode persist_mode) override;
	DUCKDB_API optional_ptr<SecretEntry> CreateSecret(ClientContext &context, const CreateSecretInfo &info) override;
	DUCKDB_API BoundStatement BindCreateSecret(CatalogTransaction transaction, CreateSecretStatement &stmt) override;
	DUCKDB_API optional_ptr<SecretEntry> GetSecretByPath(CatalogTransaction transaction, const string &path,
	                                                     const string &type) override;
	DUCKDB_API optional_ptr<SecretEntry> GetSecretByName(CatalogTransaction transaction, const string &name) override;
	DUCKDB_API void DropSecretByName(CatalogTransaction transaction, const string &name, bool missing_ok) override;
	DUCKDB_API vector<SecretEntry *> AllSecrets(CatalogTransaction transaction) override;
	DUCKDB_API bool AllowConfigChanges() override;

	//! Return secret directory
	string GetSecretDirectory(DBConfig &config);

private:
	//! Deserialize a secret
	unique_ptr<BaseSecret> DeserializeSecretInternal(CatalogTransaction transaction, Deserializer &deserializer);
	//! Lookup a SecretType
	SecretType LookupTypeInternal(CatalogTransaction transaction, const string &type);
	//! Lookup a CreateSecretFunction
	CreateSecretFunction *LookupFunctionInternal(CatalogTransaction transaction, const string &type,
	                                             const string &provider);
	//! Register a new Secret
	optional_ptr<SecretEntry> RegisterSecretInternal(CatalogTransaction transaction,
	                                                 unique_ptr<const BaseSecret> secret, OnCreateConflict on_conflict,
	                                                 SecretPersistMode persist_mode);
	//! Write a secret to the FileSystem
	void WriteSecretToFile(CatalogTransaction transaction, const BaseSecret &secret);

	//! Initialize the secret catalog_set and permanent secrets (lazily)
	void InitializeSecrets(CatalogTransaction transaction);
	//! Lazily preloads the permanent secrets
	void LoadPermanentSecretsMap(CatalogTransaction transaction);

	//! Autoload extension for specific secret type
	void AutoloadExtensionForType(ClientContext &context, const string &type);
	//! Autoload extension for specific secret function
	void AutoloadExtensionForFunction(ClientContext &context, const string &type, const string &provider);

	//! Secret CatalogSets
	unique_ptr<CatalogSet> secrets;
	unique_ptr<CatalogSet> secret_types;
	unique_ptr<CatalogSet> secret_functions;

	//! Lock for initialization
	mutex initialize_lock;
	//! Map of permanent serret files: read once during initialization
	case_insensitive_map_t<string> permanent_secret_files;
	//! The secret manager is fully initialized: no more settings changes or fs reading
	atomic<bool> initialized {false};
};

//! The DefaultGenerator for permanent secrets. This is used to store lazy loaded secrets in the catalog
class DefaultDuckSecretGenerator : public DefaultGenerator {
public:
	DefaultDuckSecretGenerator(Catalog &catalog, DuckSecretManager &secret_manager, vector<string> &permanent_secrets);

public:
	unique_ptr<CatalogEntry> CreateDefaultEntry(ClientContext &context, const string &entry_name) override;
	vector<string> GetDefaultEntries() override;

protected:
	DuckSecretManager &secret_manager;
	vector<string> permanent_secrets;
};

} // namespace duckdb
