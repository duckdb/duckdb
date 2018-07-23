
#pragma once

#include <memory>

#include "common/printable.hpp"

namespace duckdb {

class Catalog;

class AbstractCatalogEntry : public Printable {
  public:
	AbstractCatalogEntry(Catalog *catalog, std::string name)
	    : catalog(catalog), name(name) {}

	virtual ~AbstractCatalogEntry() {}

	Catalog *catalog;
	std::string name;
};
}
