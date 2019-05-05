//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/enums/catalog_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Catalog Types
//===--------------------------------------------------------------------===//
enum class CatalogType : uint8_t {
	INVALID = 0,
	TABLE = 1,
	SCHEMA = 2,
	TABLE_FUNCTION = 3,
	SCALAR_FUNCTION = 4,
	VIEW = 5,
	INDEX = 6,
	UPDATED_ENTRY = 10,
	DELETED_ENTRY = 11,
	PREPARED_STATEMENT = 12,
	SEQUENCE = 13
};

} // namespace duckdb
