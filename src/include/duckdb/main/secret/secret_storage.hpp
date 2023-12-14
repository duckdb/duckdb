//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/secret/secret_storage.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"

namespace duckdb {

class BaseSecret;
class CatalogSet;
struct CatalogTransaction;
struct SecretMatch;
struct SecretEntry;

//! Base class for SecretStorage API
class SecretStorage {
public:
	SecretStorage(const string& name_p) : name(name_p) {};
	virtual ~SecretStorage() = default;

public:
	//! SecretStorage API

	//! Get the storage name (e.g. local_file, :memory:)
	virtual string &GetName() { return name; } ;
	//! Whether the storage hold a specific secret
	virtual bool HasSecret(CatalogTransaction transaction, const string &name) = 0;
	//! Store a secret
	virtual optional_ptr<SecretEntry> StoreSecret(CatalogTransaction transaction, unique_ptr<const BaseSecret> secret, OnCreateConflict on_conflict) = 0;
	//! Get all secrets
	virtual vector<reference<SecretEntry>> AllSecrets(CatalogTransaction transaction) = 0;
	//! Drop secret by name
	virtual void DropSecretByName(CatalogTransaction transaction, const string &name, OnEntryNotFound on_entry_not_found) = 0;
	//! Get best match
	virtual SecretMatch GetSecretByPath(CatalogTransaction transaction, const string &path, const string &type) = 0;
	//! Get a secret by name
	virtual optional_ptr<SecretEntry> GetSecretByName(CatalogTransaction transaction, const string &name) = 0;

protected:
	//! Name of the storage backend (e.g. temporary, file, etc)
	string name;
	//! Whether this storage will survive reboots
	bool persistent;
};

//! Base class for catalog set based secret storage
class CatalogSetSecretStorage : public SecretStorage {
public:
	CatalogSetSecretStorage(const string& name_p) : SecretStorage(name_p) {};

public:
	//! SecretStorage API
	string &GetName() override { return name; };
	bool HasSecret(CatalogTransaction transaction, const string &name) override;
	optional_ptr<SecretEntry> StoreSecret(CatalogTransaction transaction, unique_ptr<const BaseSecret> secret, OnCreateConflict on_conflict) override;
	vector<reference<SecretEntry>> AllSecrets(CatalogTransaction transaction)  override;
	void DropSecretByName(CatalogTransaction transaction, const string &name, OnEntryNotFound on_entry_not_found)  override;
	SecretMatch GetSecretByPath(CatalogTransaction transaction, const string &path, const string &type) override;
	optional_ptr<SecretEntry> GetSecretByName(CatalogTransaction transaction, const string &name) override;

protected:
	//! Callback called on Store to allow child classes to implement persistence.
	virtual void WriteSecret(CatalogTransaction transaction, const BaseSecret &secret);
	virtual void RemoveSecret(CatalogTransaction transaction, const string &name);

	//! CatalogSet containing the secrets
	unique_ptr<CatalogSet> secrets;
};

class TemporarySecretStorage : public CatalogSetSecretStorage {
public:
	TemporarySecretStorage(const string& name_p, DatabaseInstance& db) : CatalogSetSecretStorage(name_p) {
		secrets = make_uniq<CatalogSet>(Catalog::GetSystemCatalog(db));
		persistent = false;
	}
};

class LocalFileSecretStorage : public CatalogSetSecretStorage {
public:
	LocalFileSecretStorage(SecretManager& manager, DatabaseInstance& db, const string& name_p, const string& secret_path);

protected:
	//! Implements the actual writes to disk
	void WriteSecret(CatalogTransaction transaction, const BaseSecret &secret) override;
	//! Implements the deletes from disk
	virtual void RemoveSecret(CatalogTransaction transaction, const string &secret) override;

	//! Set of persistent secrets that are lazily loaded
	case_insensitive_set_t persistent_secrets;
	//! Path that is searched for secrets;
	string secret_path;
};

} // namespace duckdb
