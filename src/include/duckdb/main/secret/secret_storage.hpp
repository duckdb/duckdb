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
	friend class SecretManager;

public:
	SecretStorage(const string &name) : storage_name(name), persistent(false) {};
	virtual ~SecretStorage() = default;

public:
	//! SecretStorage API

	//! Get the storage name (e.g. local_file, :memory:)
	virtual string &GetName() {
		return storage_name;
	};

	//! Store a secret
	virtual optional_ptr<SecretEntry> StoreSecret(CatalogTransaction transaction, unique_ptr<const BaseSecret> secret,
	                                              OnCreateConflict on_conflict) = 0;
	//! Get all secrets
	virtual vector<reference<SecretEntry>> AllSecrets(CatalogTransaction transaction) = 0;
	//! Drop secret by name
	virtual void DropSecretByName(CatalogTransaction transaction, const string &name,
	                              OnEntryNotFound on_entry_not_found) = 0;
	//! Get best match
	virtual SecretMatch LookupSecret(CatalogTransaction transaction, const string &path, const string &type) = 0;
	//! Get a secret by name
	virtual optional_ptr<SecretEntry> GetSecretByName(CatalogTransaction transaction, const string &name) = 0;

	//! Return the offset associated to this storage for tie-breaking secrets between storages
	virtual int64_t GetTieBreakOffset() = 0;

	//! Returns include_in_lookups, used to create secret storage
	virtual bool IncludeInLookups() {
		return true;
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

//! Base Implementation for catalog set based secret storage
class CatalogSetSecretStorage : public SecretStorage {
public:
	CatalogSetSecretStorage(const string &name_p) : SecretStorage(name_p) {};

public:
	//! SecretStorage API
	string &GetName() override {
		return storage_name;
	};
	optional_ptr<SecretEntry> StoreSecret(CatalogTransaction transaction, unique_ptr<const BaseSecret> secret,
	                                      OnCreateConflict on_conflict) override;
	vector<reference<SecretEntry>> AllSecrets(CatalogTransaction transaction) override;
	void DropSecretByName(CatalogTransaction transaction, const string &name,
	                      OnEntryNotFound on_entry_not_found) override;
	SecretMatch LookupSecret(CatalogTransaction transaction, const string &path, const string &type) override;
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
	TemporarySecretStorage(const string &name_p, DatabaseInstance &db) : CatalogSetSecretStorage(name_p) {
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
	void WriteSecret(CatalogTransaction transaction, const BaseSecret &secret) override;
	//! Implements the deletes from disk
	virtual void RemoveSecret(CatalogTransaction transaction, const string &secret) override;

	//! Set of persistent secrets that are lazily loaded
	case_insensitive_set_t persistent_secrets;
	//! Path that is searched for secrets;
	string secret_path;
};

} // namespace duckdb
