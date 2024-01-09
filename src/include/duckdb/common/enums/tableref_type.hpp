//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/tableref_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Table Reference Types
//===--------------------------------------------------------------------===//
enum class TableReferenceType : uint8_t {
	INVALID = 0,         // invalid table reference type
	BASE_TABLE = 1,      // base table reference
	SUBQUERY = 2,        // output of a subquery
	JOIN = 3,            // output of join
	TABLE_FUNCTION = 5,  // table producing function
	EXPRESSION_LIST = 6, // expression list
	CTE = 7,             // Recursive CTE
	EMPTY = 8,           // placeholder for empty FROM
	PIVOT = 9            // pivot statement
};

} // namespace duckdb
