
#pragma once

#include <memory>

#include "common/printable.hpp"

namespace duckdb {

class AbstractCatalogEntry
    : public Printable {
  public:
	AbstractCatalogEntry(std::string name) : name(name) {}

	virtual ~AbstractCatalogEntry() {}

	std::string name;
};
}
