//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/collation_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

//! EXPLICIT_COLLATIONS applies the collations of the type, but not the default collation
enum class CollationType { ALL_COLLATIONS, COMBINABLE_COLLATIONS, EXPLICIT_COLLATIONS };

} // namespace duckdb
