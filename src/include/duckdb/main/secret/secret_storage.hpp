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
class Catalog;
class CatalogSet;
struct CatalogTransaction;
class DatabaseInstance;
struct SecretMatch;
struct SecretEntry;

//! Base class for SecretStorage API
class SecretStorage {
	friend class SecretManager;

public:
	explicit SecretStorage(const string &name) : storage_name(name), persistent(false) {};
	virtual ~SecretStorage() = default;

public:
	//! SecretStorage API

	//! Get the storage name (e.g. local_file, :memory:)
	virtual string &GetName() {
		return storage_name;
	};

	//! Store a secret
	virtual unique_ptr<SecretEntry> StoreSecret(unique_ptr<const BaseSecret> secret, OnCreateConflict on_conflict,
	                                            optional_ptr<CatalogTransaction> transaction = nullptr) = 0;
	//! Get all secrets
	virtual vector<SecretEntry> AllSecrets(optional_ptr<CatalogTransaction> transaction = nullptr) = 0;
	//! Drop secret by name
	virtual void DropSecretByName(const string &name, OnEntryNotFound on_entry_not_found,
	                              optional_ptr<CatalogTransaction> transaction = nullptr) = 0;
	//! Get best match
	virtual SecretMatch LookupSecret(const string &path, const string &type,
	                                 optional_ptr<CatalogTransaction> transaction = nullptr) = 0;
	//! Get a secret by name
	virtual unique_ptr<SecretEntry> GetSecretByName(const string &name,
	                                                optional_ptr<CatalogTransaction> transaction = nullptr) = 0;

	//! Return the offset associated to this storage for tie-breaking secrets between storages
	virtual int64_t GetTieBreakOffset() = 0;

	//! Returns include_in_lookups, used to create secret storage
	virtual bool IncludeInLookups() {
		return true;
	}

	virtual bool Persistent() const {
		return persistent;
	}

protected:
	//! Helper function to select the best matching secret within a storage. Tie-breaks within a storage are broken
	//! by secret name by default.
	SecretMatch SelectBestMatch(SecretEntry &secret_entry, const string &path, SecretMatch &current_best);

	//! Offsets the score to tie-break secrets giving preference to the storage with the lowest storage_penalty
	//! the base implementation will be chosen last in a tie-break
	int64_t OffsetMatchScore(int64_t score) {
		return 100 * score - GetTieBreakOffset();
	}

	//! Name of the storage backend (e.g. temporary, file, etc)
	string storage_name;
	//! Whether entries in this storage will survive duckdb reboots
	bool persistent;
};

//! Wrapper struct around a SecretEntry to allow storing it
struct SecretCatalogEntry : public InCatalogEntry {
public:
	SecretCatalogEntry(unique_ptr<SecretEntry> secret_p, Catalog &catalog);
	SecretCatalogEntry(unique_ptr<const BaseSecret> secret_p, Catalog &catalog);

	//! The secret to store in a catalog
	unique_ptr<SecretEntry> secret;
};

//! Base Implementation for catalog set based secret storage
class CatalogSetSecretStorage : public SecretStorage {
public:
	CatalogSetSecretStorage(DatabaseInstance &db_instance, const string &name_p)
	    : SecretStorage(name_p), db(db_instance) {};

public:
	//! SecretStorage API
	string &GetName() override {
		return storage_name;
	};

	unique_ptr<SecretEntry> StoreSecret(unique_ptr<const BaseSecret> secret, OnCreateConflict on_conflict,
	                                    optional_ptr<CatalogTransaction> transaction = nullptr) override;
	vector<SecretEntry> AllSecrets(optional_ptr<CatalogTransaction> transaction = nullptr) override;
	void DropSecretByName(const string &name, OnEntryNotFound on_entry_not_found,
	                      optional_ptr<CatalogTransaction> transaction = nullptr) override;
	SecretMatch LookupSecret(const string &path, const string &type,
	                         optional_ptr<CatalogTransaction> transaction = nullptr) override;
	unique_ptr<SecretEntry> GetSecretByName(const string &name,
	                                        optional_ptr<CatalogTransaction> transaction = nullptr) override;

protected:
	//! Callback called on Store to allow child classes to implement persistence.
	virtual void WriteSecret(const BaseSecret &secret, OnCreateConflict on_conflict);
	virtual void RemoveSecret(const string &name, OnEntryNotFound on_entry_not_found);
	//! Returns the CatalogTransaction in `transaction` if not set, return the System transaction
	CatalogTransaction GetTransactionOrDefault(optional_ptr<CatalogTransaction> transaction);

	//! CatalogSet containing the secrets
	unique_ptr<CatalogSet> secrets;
	//! DB instance for accessing the system catalog transaction
	DatabaseInstance &db;
};

class TemporarySecretStorage : public CatalogSetSecretStorage {
public:
	TemporarySecretStorage(const string &name_p, DatabaseInstance &db_p) : CatalogSetSecretStorage(db_p, name_p) {
		secrets = make_uniq<CatalogSet>(Catalog::GetSystemCatalog(db));
		persistent = false;
	}

	int64_t GetTieBreakOffset() override {
		return 10;
	}

protected:
};

class LocalFileSecretStorage : public CatalogSetSecretStorage {
public:
	LocalFileSecretStorage(SecretManager &manager, DatabaseInstance &db, const string &name_p,
	                       const string &secret_path);

	int64_t GetTieBreakOffset() override {
		return 20;
	}

protected:
	//! Implements the writes to disk
	void WriteSecret(const BaseSecret &secret, OnCreateConflict on_conflict) override;
	//! Implements the deletes from disk
	void RemoveSecret(const string &secret, OnEntryNotFound on_entry_not_found) override;

	//! Set of persistent secrets that are lazily loaded
	case_insensitive_set_t persistent_secrets;
	//! Path that is searched for secrets;
	string secret_path;
};

} // namespace duckdb
