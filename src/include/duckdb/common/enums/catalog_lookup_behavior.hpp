//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/catalog_lookup_behavior.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//! Enum used for indicating lookup behavior of specific catalog types
// STANDARD means the catalog lookups are performed in a regular manner (i.e. according to the users' search path)
// LOWER_PRIORITY means the catalog lookups are de-prioritized and we do lookups in other catalogs first
// NEVER_LOOKUP means we never do lookups for this specific type in this catalog
enum class CatalogLookupBehavior : uint8_t { STANDARD = 0, LOWER_PRIORITY = 1, NEVER_LOOKUP = 2 };

} // namespace duckdb
