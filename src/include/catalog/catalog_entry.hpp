//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// catalog/catalog_entry.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/internal_types.hpp"
#include "common/printable.hpp"
#include "common/exception.hpp"

namespace duckdb {

struct AlterInformation;
class Catalog;
class CatalogSet;
class Transaction;

//! Abstract base class of an entry in the catalog
class CatalogEntry {
  public:
	CatalogEntry(CatalogType type, Catalog *catalog, std::string name)
	    : type(type), catalog(catalog), set(nullptr), name(name),
	      deleted(false), parent(nullptr) {
	}

	virtual ~CatalogEntry() {
	}

	//! Returns true if other objects depend on this object
	virtual bool HasDependents(Transaction &transaction) {
		return false;
	}
	//! Function that drops all dependents (used for Cascade)
	virtual void DropDependents(Transaction &transaction) {
	}

	virtual std::unique_ptr<CatalogEntry> AlterEntry(AlterInformation *info) {
		throw CatalogException("Unsupported alter type for catalog entry!");
	}

	//! The type of this catalog entry
	CatalogType type;
	//! Reference to the catalog this entry belongs to
	Catalog *catalog;
	//! Reference to the catalog set this entry is stored in
	CatalogSet *set;
	//! The name of the entry
	std::string name;
	//! Whether or not the object is deleted
	bool deleted;
	//! Timestamp at which the catalog entry was created
	transaction_t timestamp;
	//! Child entry
	std::unique_ptr<CatalogEntry> child;
	//! Parent entry (the node that owns this node)
	CatalogEntry *parent;
};
} // namespace duckdb
