
#pragma once

#include "catalog/abstract_catalog.hpp"
#include "common/internal_types.hpp"
#include "common/types/value.hpp"

namespace duckdb {

class TableCatalogEntry;

class ColumnCatalogEntry : public AbstractCatalogEntry {
  public:
	ColumnCatalogEntry(std::string name, TypeId type, bool is_not_null);
	ColumnCatalogEntry(std::string name, TypeId type, bool is_not_null,
	                   Value default_value);

	size_t oid;
	TypeId type;
	bool is_not_null;
	bool has_default;
	Value default_value;

	virtual std::string ToString() const { return std::string(); }
};
}
