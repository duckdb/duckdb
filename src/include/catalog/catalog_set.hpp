//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// catalog/catalog_set.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "common/internal_types.hpp"

#include "catalog/catalog_entry.hpp"

#include "transaction/transaction.hpp"

namespace duckdb {

//! The Catalog Set stores (key, value) map of a set of AbstractCatalogEntries
class CatalogSet {
  public:
	//! Create an entry in the catalog set. Returns whether or not it was
	//! successful.
	bool CreateEntry(Transaction &transaction, const std::string &name,
	                 std::unique_ptr<CatalogEntry> value);

	bool AlterEntry(Transaction &transaction, const std::string &name,
	                bool cascade);

	bool DropEntry(Transaction &transaction, const std::string &name,
	               bool cascade);
	//! Returns whether or not an entry exists
	bool EntryExists(Transaction &transaction, const std::string &name);
	//! Returns the entry with the specified name
	CatalogEntry *GetEntry(Transaction &transaction, const std::string &name);

	//! Rollback <entry> to be the currently valid entry for a certain catalog
	//! entry
	void Undo(CatalogEntry *entry);

	//! Drops all entries
	void DropAllEntries(Transaction &transaction);
	//! Returns true if the catalog set is empty for the transaction, false
	//! otherwise
	bool IsEmpty(Transaction &transaction);

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

  private:
	//! Drops an entry from the catalog set; must hold the catalog_lock to
	//! safely call this
	bool DropEntry(Transaction &transaction, CatalogEntry &entry, bool cascade);
	//! Given a root entry, gets the entry valid for this transaction
	CatalogEntry *GetEntryForTransaction(Transaction &transaction,
	                                     CatalogEntry *current);
	//! The catalog lock is used to make changes to the data
	std::mutex catalog_lock;
	//! The set of entries present in the CatalogSet.
	std::unordered_map<std::string, std::unique_ptr<CatalogEntry>> data;
};

} // namespace duckdb
