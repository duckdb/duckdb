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
struct CatalogTransaction;

//! Wrapper around a BaseSecret containing metadata and allow storing in CatalogSet
struct SecretEntry : public InCatalogEntry {
public:
	SecretEntry(unique_ptr<const BaseSecret> secret, Catalog &catalog, string name)
	    : InCatalogEntry(CatalogType::SECRET_ENTRY, catalog, name), secret(std::move(secret)) {
		internal = true;
	}

	//! Metadata for user on how the secret is stored. (DuckSecretManager will set this to the path)
	string storage_mode;
	//! The secret pointer
	unique_ptr<const BaseSecret> secret;
};

//! Secret Manager is responsible the for the creation, deletion and storage of secrets.
class SecretManager {
	friend struct SecretEntry;

public:
	virtual ~SecretManager() = default;

	//! Static Helper Functions
	DUCKDB_API static SecretManager &Get(ClientContext &context);

	//! Secret Manager API
	//! Initializes the secret manager for a given DB instance
	DUCKDB_API virtual void Initialize(DatabaseInstance &db) = 0;
	//! Deserialize the secret. Will look up the deserialized type, then call the deserialize for the registered type.
	DUCKDB_API virtual unique_ptr<BaseSecret> DeserializeSecret(CatalogTransaction transaction,
	                                                            Deserializer &deserializer) = 0;
	//! Registers a secret type
	DUCKDB_API virtual void RegisterSecretType(CatalogTransaction transaction, SecretType &type) = 0;
	//! Get the registered type
	DUCKDB_API virtual SecretType LookupType(CatalogTransaction transaction, const string &type) = 0;
	//! Registers a create secret function
	DUCKDB_API virtual void RegisterSecretFunction(CatalogTransaction transaction, CreateSecretFunction function,
	                                               OnCreateConflict on_conflict) = 0;
	//! Register a Secret directly
	DUCKDB_API virtual optional_ptr<SecretEntry> RegisterSecret(CatalogTransaction transaction,
	                                                            unique_ptr<const BaseSecret> secret,
	                                                            OnCreateConflict on_conflict,
	                                                            SecretPersistMode persist_mode) = 0;
	//! Create & Register a secret by looking up the function
	DUCKDB_API virtual optional_ptr<SecretEntry> CreateSecret(ClientContext &context,
	                                                          const CreateSecretInfo &input) = 0;
	//! Binds a create secret statement, optionally pass a ClientContext to support auto-loading extensions
	DUCKDB_API virtual BoundStatement BindCreateSecret(CatalogTransaction transaction, CreateSecretInfo &info) = 0;
	//! Get the secret whose scope best matches the path.
	DUCKDB_API virtual optional_ptr<SecretEntry> GetSecretByPath(CatalogTransaction transaction, const string &path,
	                                                             const string &type) = 0;
	//! Get a secret by name
	DUCKDB_API virtual optional_ptr<SecretEntry> GetSecretByName(CatalogTransaction transaction,
	                                                             const string &name) = 0;
	//! Drop a secret by name
	DUCKDB_API virtual void DropSecretByName(CatalogTransaction transaction, const string &name,
	                                         OnEntryNotFound on_entry_not_found) = 0;
	//! Get a vector of all registered secrets
	DUCKDB_API virtual vector<reference<SecretEntry>> AllSecrets(CatalogTransaction transaction) = 0;
	//! Whether permanent secrets are enabled
	DUCKDB_API virtual void SetEnablePermanentSecrets(bool enabled) = 0;
	DUCKDB_API virtual void ResetEnablePermanentSecrets() = 0;
	DUCKDB_API virtual bool PermanentSecretsEnabled() = 0;
	//! Where to store the permanent secrets
	DUCKDB_API virtual void SetPermanentSecretPath(const string &path) = 0;
	DUCKDB_API virtual void ResetPermanentSecretPath() = 0;
	DUCKDB_API virtual string PermanentSecretPath() = 0;

	//! Utility functions
	DUCKDB_API virtual void DropSecretByName(ClientContext &context, const string &name,
	                                         OnEntryNotFound on_entry_not_found);
};

} // namespace duckdb
