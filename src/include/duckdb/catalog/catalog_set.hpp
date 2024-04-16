//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/default/default_generator.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/catalog/similar_catalog_entry.hpp"
#include <functional>
#include <memory>

namespace duckdb {
struct AlterInfo;

class ClientContext;
class LogicalDependencyList;

class DuckCatalog;
class TableCatalogEntry;
class SequenceCatalogEntry;

class CatalogEntryMap {
public:
	CatalogEntryMap() {
	}

public:
	void AddEntry(unique_ptr<CatalogEntry> entry);
	void UpdateEntry(unique_ptr<CatalogEntry> entry);
	void DropEntry(CatalogEntry &entry);
	case_insensitive_tree_t<unique_ptr<CatalogEntry>> &Entries();
	optional_ptr<CatalogEntry> GetEntry(const string &name);

private:
	//! Mapping of string to catalog entry
	case_insensitive_tree_t<unique_ptr<CatalogEntry>> entries;
};

//! The Catalog Set stores (key, value) map of a set of CatalogEntries
class CatalogSet {
public:
	struct EntryLookup {
		enum class FailureReason { SUCCESS, DELETED, NOT_PRESENT };
		optional_ptr<CatalogEntry> result;
		FailureReason reason;
	};

public:
	DUCKDB_API explicit CatalogSet(Catalog &catalog, unique_ptr<DefaultGenerator> defaults = nullptr);
	~CatalogSet();

	//! Create an entry in the catalog set. Returns whether or not it was
	//! successful.
	DUCKDB_API bool CreateEntry(CatalogTransaction transaction, const string &name, unique_ptr<CatalogEntry> value,
	                            const LogicalDependencyList &dependencies);
	DUCKDB_API bool CreateEntry(ClientContext &context, const string &name, unique_ptr<CatalogEntry> value,
	                            const LogicalDependencyList &dependencies);

	DUCKDB_API bool AlterEntry(CatalogTransaction transaction, const string &name, AlterInfo &alter_info);

	DUCKDB_API bool DropEntry(CatalogTransaction transaction, const string &name, bool cascade,
	                          bool allow_drop_internal = false);
	DUCKDB_API bool DropEntry(ClientContext &context, const string &name, bool cascade,
	                          bool allow_drop_internal = false);

	DUCKDB_API DuckCatalog &GetCatalog();

	bool AlterOwnership(CatalogTransaction transaction, ChangeOwnershipInfo &info);

	void CleanupEntry(CatalogEntry &catalog_entry);

	//! Returns the entry with the specified name
	DUCKDB_API EntryLookup GetEntryDetailed(CatalogTransaction transaction, const string &name);
	DUCKDB_API optional_ptr<CatalogEntry> GetEntry(CatalogTransaction transaction, const string &name);
	DUCKDB_API optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const string &name);

	//! Gets the entry that is most similar to the given name (i.e. smallest levenshtein distance), or empty string if
	//! none is found. The returned pair consists of the entry name and the distance (smaller means closer).
	SimilarCatalogEntry SimilarEntry(CatalogTransaction transaction, const string &name);

	//! Rollback <entry> to be the currently valid entry for a certain catalog
	//! entry
	void Undo(CatalogEntry &entry);

	//! Scan the catalog set, invoking the callback method for every committed entry
	DUCKDB_API void Scan(const std::function<void(CatalogEntry &)> &callback);
	//! Scan the catalog set, invoking the callback method for every entry
	DUCKDB_API void ScanWithPrefix(CatalogTransaction transaction, const std::function<void(CatalogEntry &)> &callback,
	                               const string &prefix);
	DUCKDB_API void Scan(CatalogTransaction transaction, const std::function<void(CatalogEntry &)> &callback);
	DUCKDB_API void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);

	template <class T>
	vector<reference<T>> GetEntries(CatalogTransaction transaction) {
		vector<reference<T>> result;
		Scan(transaction, [&](CatalogEntry &entry) { result.push_back(entry.Cast<T>()); });
		return result;
	}

	DUCKDB_API bool CreatedByOtherActiveTransaction(CatalogTransaction transaction, transaction_t timestamp);
	DUCKDB_API bool CommittedAfterStarting(CatalogTransaction transaction, transaction_t timestamp);
	DUCKDB_API bool HasConflict(CatalogTransaction transaction, transaction_t timestamp);
	DUCKDB_API bool UseTimestamp(CatalogTransaction transaction, transaction_t timestamp);

	void UpdateTimestamp(CatalogEntry &entry, transaction_t timestamp);

	mutex &GetCatalogLock() {
		return catalog_lock;
	}

	void Verify(Catalog &catalog);

private:
	bool DropDependencies(CatalogTransaction transaction, const string &name, bool cascade,
	                      bool allow_drop_internal = false);
	//! Given a root entry, gets the entry valid for this transaction
	CatalogEntry &GetEntryForTransaction(CatalogTransaction transaction, CatalogEntry &current);
	CatalogEntry &GetCommittedEntry(CatalogEntry &current);
	optional_ptr<CatalogEntry> GetEntryInternal(CatalogTransaction transaction, const string &name);
	optional_ptr<CatalogEntry> CreateCommittedEntry(unique_ptr<CatalogEntry> entry);

	//! Create all default entries
	void CreateDefaultEntries(CatalogTransaction transaction, unique_lock<mutex> &lock);
	//! Attempt to create a default entry with the specified name. Returns the entry if successful, nullptr otherwise.
	optional_ptr<CatalogEntry> CreateDefaultEntry(CatalogTransaction transaction, const string &name,
	                                              unique_lock<mutex> &lock);

	bool DropEntryInternal(CatalogTransaction transaction, const string &name, bool allow_drop_internal = false);

	bool CreateEntryInternal(CatalogTransaction transaction, const string &name, unique_ptr<CatalogEntry> value,
	                         unique_lock<mutex> &read_lock, bool should_be_empty = true);
	void CheckCatalogEntryInvariants(CatalogEntry &value, const string &name);
	//! Verify that the previous entry in the chain is dropped.
	bool VerifyVacancy(CatalogTransaction transaction, CatalogEntry &entry);
	//! Start the catalog entry chain with a dummy node
	bool StartChain(CatalogTransaction transaction, const string &name, unique_lock<mutex> &read_lock);
	bool RenameEntryInternal(CatalogTransaction transaction, CatalogEntry &old, const string &new_name,
	                         AlterInfo &alter_info, unique_lock<mutex> &read_lock);

private:
	DuckCatalog &catalog;
	//! The catalog lock is used to make changes to the data
	mutex catalog_lock;
	CatalogEntryMap map;
	//! The generator used to generate default internal entries
	unique_ptr<DefaultGenerator> defaults;
};
} // namespace duckdb
