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
	TABLE_ENTRY = 1,
	SCHEMA_ENTRY = 2,
	TABLE_FUNCTION_ENTRY = 3,
	SCALAR_FUNCTION_ENTRY = 4,
	AGGREGATE_FUNCTION_ENTRY = 5,
	COPY_FUNCTION = 6,
	VIEW_ENTRY = 7,
	INDEX_ENTRY = 8,
	PREPARED_STATEMENT = 9,
	SEQUENCE_ENTRY = 10,
	COLLATION_ENTRY = 11,

	UPDATED_ENTRY = 50,
	DELETED_ENTRY = 51,
};

string CatalogTypeToString(CatalogType type);

} // namespace duckdb
