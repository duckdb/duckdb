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

#include <functional>
#include <memory>
#include <mutex>

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
		for (auto &kv : data) {
			auto entry = kv.second.get();
			entry = GetEntryForTransaction(transaction, entry);
			if (!entry->deleted) {
				callback(entry);
			}
		}
	}

	static bool HasConflict(Transaction &transaction, CatalogEntry &current);

private:
	//! Drops an entry from the catalog set; must hold the catalog_lock to
	//! safely call this
	void DropEntryInternal(Transaction &transaction, CatalogEntry &entry, bool cascade, set_lock_map_t &lock_set);
	//! Given a root entry, gets the entry valid for this transaction
	CatalogEntry *GetEntryForTransaction(Transaction &transaction, CatalogEntry *current);

	Catalog &catalog;
	//! The catalog lock is used to make changes to the data
	std::mutex catalog_lock;
	//! The set of entries present in the CatalogSet.
	unordered_map<string, unique_ptr<CatalogEntry>> data;
};

} // namespace duckdb
