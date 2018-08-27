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

#include <memory>
#include <string>
#include <unordered_map>
#include <mutex>

#include "common/internal_types.hpp"

#include "catalog/abstract_catalog.hpp"

#include "transaction/transaction.hpp"

namespace duckdb {

//! The Catalog Set stores (key, value) map of a set of AbstractCatalogEntries
class CatalogSet {
  public:
	//! Create an entry in the catalog set. Returns whether or not it was
	//! successful.
	bool CreateEntry(Transaction &transaction, const std::string &name,
	                 std::unique_ptr<AbstractCatalogEntry> value);
	//! Returns whether or not an entry exists
	bool EntryExists(Transaction &transaction, const std::string &name);
	//! Returns the entry with the specified name
	AbstractCatalogEntry *GetEntry(Transaction &transaction,
	                               const std::string &name);

	//! Rollback <entry> to be the currently valid entry for a certain catalog
	//! entry
	void Undo(AbstractCatalogEntry *entry);

  private:
	//! The catalog lock is used to make changes to the data
	std::mutex catalog_lock;
	//! The set of entries present in the CatalogSet.
	std::unordered_map<std::string, std::unique_ptr<AbstractCatalogEntry>> data;
};

} // namespace duckdb