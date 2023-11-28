//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/secret/secret_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/main/secret/secret.hpp"

namespace duckdb {
struct CreateSecretInfo;
struct BoundStatement;
class CreateSecretStatement;
struct CatalogTransaction;

//! SecretEntry is a wrapper around a secret containing metadata from the secret manager and allowing storage in a
//! CatalogSet
struct SecretEntry : public CatalogEntry {
public:
	SecretEntry(unique_ptr<const BaseSecret> secret, Catalog &catalog, string name)
	    : CatalogEntry(CatalogType::SECRET, catalog, name), secret(std::move(secret)), parent_catalog(&catalog) {
		internal = true;
	}

	Catalog &ParentCatalog() {
		return *parent_catalog;
	};

	//! Metadata for user on how the secret is stored. (DuckSecretManager will set this to the path)
	string storage_mode;
	//! The secret pointer
	shared_ptr<const BaseSecret> secret;

	optional_ptr<Catalog> parent_catalog;
};

//! Secret Manager is responsible the for the creation, deletion and storage of secrets.
class SecretManager {
	friend struct SecretEntry;

public:
	virtual ~SecretManager() = default;

	//! Initializes the secret manager for a given DB instance
	DUCKDB_API virtual void Initialize(DatabaseInstance &db) = 0;
	//! Deserialize the secret. Will look up the deserialized type, then call the deserialize for the registered type.
	DUCKDB_API virtual unique_ptr<BaseSecret> DeserializeSecret(CatalogTransaction transaction,
	                                                            Deserializer &deserializer) = 0;
	//! Registers a secret type
	DUCKDB_API virtual void RegisterSecretType(SecretType &type) = 0;
	//! Get the registered type
	DUCKDB_API virtual SecretType LookupType(const string &type) = 0;
	//! Registers a create secret function
	DUCKDB_API virtual void RegisterSecretFunction(CreateSecretFunction function, OnCreateConflict on_conflict) = 0;
	//! Register a Secret directly
	DUCKDB_API virtual optional_ptr<SecretEntry> RegisterSecret(CatalogTransaction transaction,
	                                                            unique_ptr<const BaseSecret> secret,
	                                                            OnCreateConflict on_conflict,
	                                                            SecretPersistMode persist_mode) = 0;
	//! Create & Register a secret by looking up the function
	DUCKDB_API virtual optional_ptr<SecretEntry> CreateSecret(ClientContext &context,
	                                                          const CreateSecretInfo &input) = 0;
	//! Binds a create secret statement
	DUCKDB_API virtual BoundStatement BindCreateSecret(CreateSecretStatement &stmt) = 0;
	//! Get the secret whose scope best matches the path.
	DUCKDB_API virtual optional_ptr<SecretEntry> GetSecretByPath(CatalogTransaction transaction, const string &path,
	                                                             const string &type) = 0;
	//! Get a secret by name
	DUCKDB_API virtual optional_ptr<SecretEntry> GetSecretByName(CatalogTransaction transaction,
	                                                             const string &name) = 0;
	//! Drop a secret by name
	DUCKDB_API virtual void DropSecretByName(CatalogTransaction transaction, const string &name, bool missing_ok) = 0;
	//! Get a vector of all registered secrets
	DUCKDB_API virtual vector<SecretEntry *> AllSecrets(CatalogTransaction transaction) = 0;
	//! Returns a boolean indicating the SecretManager has been initialized and no longer excepts certain config changes
	DUCKDB_API virtual bool AllowConfigChanges() = 0;
};

//! The debug secret manager demonstrates how the Base Secret Manager can be extended
class DebugSecretManager : public SecretManager {
public:
	~DebugSecretManager() override = default;
	DebugSecretManager(unique_ptr<SecretManager> secret_manager) : base_secret_manager(std::move(secret_manager)) {};

	DUCKDB_API void Initialize(DatabaseInstance &db) override;
	DUCKDB_API unique_ptr<BaseSecret> DeserializeSecret(CatalogTransaction transaction,
	                                                    Deserializer &deserializer) override;
	DUCKDB_API void RegisterSecretType(SecretType &type) override;
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
	DUCKDB_API SecretType LookupType(const string &type) override;
	DUCKDB_API vector<SecretEntry *> AllSecrets(CatalogTransaction transaction) override;
	DUCKDB_API bool AllowConfigChanges() override;

protected:
	unique_ptr<SecretManager> base_secret_manager;
};

} // namespace duckdb
