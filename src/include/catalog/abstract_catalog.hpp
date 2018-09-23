//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// catalog/abstract_catalog.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/internal_types.hpp"
#include "common/printable.hpp"

namespace duckdb {

class Catalog;
class CatalogSet;

//! Abstract base class of an entry in the catalog
class AbstractCatalogEntry {
  public:
	AbstractCatalogEntry(CatalogType type, Catalog *catalog, std::string name)
	    : type(type), catalog(catalog), name(name), deleted(false),
	      parent(nullptr), set(nullptr) {}

	virtual ~AbstractCatalogEntry() {}

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
	std::unique_ptr<AbstractCatalogEntry> child;
	//! Parent entry (the node that owns this node)
	AbstractCatalogEntry *parent;
};
} // namespace duckdb
