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

#include <memory>

#include "common/printable.hpp"

namespace duckdb {

class Catalog;

//! Abstract base class of an entry in the catalog
class AbstractCatalogEntry : public Printable {
  public:
	AbstractCatalogEntry(Catalog *catalog, std::string name)
	    : catalog(catalog), name(name) {}

	virtual ~AbstractCatalogEntry() {}

	//! Reference to the catalog this entry belongs to
	Catalog *catalog;
	//! The name of the entry
	std::string name;
};
} // namespace duckdb
