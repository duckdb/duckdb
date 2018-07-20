
#include "catalog/column_catalog.hpp"

using namespace duckdb;
using namespace std;

ColumnCatalogEntry::ColumnCatalogEntry(string name, TypeId type,
                                       bool is_not_null)
    : AbstractCatalogEntry(nullptr, name), type(type), is_not_null(is_not_null),
      has_default(false) {}

ColumnCatalogEntry::ColumnCatalogEntry(string name, TypeId type,
                                       bool is_not_null, Value default_value)
    : AbstractCatalogEntry(nullptr, name), type(type), is_not_null(is_not_null),
      has_default(true), default_value(default_value) {}

ColumnCatalogEntry::ColumnCatalogEntry(
    const ColumnCatalogEntry &base)
    : AbstractCatalogEntry(nullptr, base.name) {}