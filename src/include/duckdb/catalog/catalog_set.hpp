//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/mutex.hpp"

#include <functional>
#include <memory>

namespace duckdb {
struct AlterInfo;

class Transaction;

typedef unordered_map<CatalogSet *, std::unique_lock<std::mutex>> set_lock_map_t;

//! The Catalog Set stores (key, value) map of a set of AbstractCatalogEntries
class CatalogSet {
	friend class DependencyManager;

public:
	CatalogSet(Catalog &catalog);

	//! Create an entry in the catalog set. Returns whether or not it was
	//! successful.
	bool CreateEntry(Transaction &transaction, const string &name, unique_ptr<CatalogEntry> value,
	                 unordered_set<CatalogEntry *> &dependencies);

	//! Creates an entry in the catalog without any transactional semantics (i.e. it is instantly committed)
	//! Returns either the entry as written, or the entry that was already there (if any)
	CatalogEntry* CreateEntry(const string &name, unique_ptr<CatalogEntry> value);

	bool AlterEntry(ClientContext &context, const string &name, AlterInfo *alter_info);

	bool DropEntry(Transaction &transaction, const string &name, bool cascade);
	//! Returns the entry with the specified name
	CatalogEntry *GetEntry(Transaction &transaction, const string &name);
	//! Returns the root entry with the specified name regardless of transaction (or nullptr if there are none)
	CatalogEntry *GetRootEntry(const string &name);

	//! Rollback <entry> to be the currently valid entry for a certain catalog
	//! entry
	void Undo(CatalogEntry *entry);

	//! Scan the catalog set, invoking the callback method for every entry
	template <class T> void Scan(Transaction &transaction, T &&callback) {
		// lock the catalog set
		std::lock_guard<std::mutex> lock(catalog_lock);
		for (auto &kv : entries) {
			auto entry = kv.second.get();
			entry = GetEntryForTransaction(transaction, entry);
			if (!entry->deleted) {
				callback(entry);
			}
		}
	}

	static bool HasConflict(Transaction &transaction, CatalogEntry &current);

	idx_t GetEntryIndex(CatalogEntry *entry);
	CatalogEntry *GetEntryFromIndex(idx_t index);
	void ClearEntryName(string name);

private:
	//! Given a root entry, gets the entry valid for this transaction
	CatalogEntry *GetEntryForTransaction(Transaction &transaction, CatalogEntry *current);
	bool GetEntryInternal(Transaction &transaction, const string &name, idx_t &entry_index, CatalogEntry *&entry);
	bool GetEntryInternal(Transaction &transaction, idx_t entry_index, CatalogEntry *&entry);
	//! Drops an entry from the catalog set; must hold the catalog_lock to safely call this
	void DropEntryInternal(Transaction &transaction, idx_t entry_index, CatalogEntry &entry, bool cascade,
	                       set_lock_map_t &lock_set);

private:
	Catalog &catalog;
	//! The catalog lock is used to make changes to the data
	mutex catalog_lock;
	//! Mapping of string to catalog entry
	unordered_map<string, idx_t> mapping;
	//! The set of catalog entries
	unordered_map<idx_t, unique_ptr<CatalogEntry>> entries;
	//! The current catalog entry index
	idx_t current_entry = 0;
};

} // namespace duckdb
