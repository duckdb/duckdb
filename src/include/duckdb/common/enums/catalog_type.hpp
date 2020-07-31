//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/catalog_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

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
	AGGREGATE_FUNCTION = 5,
	COPY_FUNCTION = 6,
	VIEW = 7,
	INDEX = 8,
	PREPARED_STATEMENT = 9,
	SEQUENCE = 10,
	COLLATION = 11,

	UPDATED_ENTRY = 50,
	DELETED_ENTRY = 51,
};

string CatalogTypeToString(CatalogType type);

} // namespace duckdb
