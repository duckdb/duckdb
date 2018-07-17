
#pragma once

#include "common/printable.hpp"

namespace duckdb {

class AbstractCatalogEntry
    : public Printable,
      public std::enable_shared_from_this<AbstractCatalogEntry> {
  public:
	AbstractCatalogEntry(std::string name) : name(name) {}
	AbstractCatalogEntry(std::string name,
	                     std::shared_ptr<AbstractCatalogEntry> parent)
	    : name(name), parent(parent) {}
	virtual ~AbstractCatalogEntry() {}

	std::string name;
	std::weak_ptr<AbstractCatalogEntry> parent;
};
}
