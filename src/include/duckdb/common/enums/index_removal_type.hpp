//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/index_removal_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

enum class IndexRemovalType {
	MAIN_INDEX,
	DELETED_ROWS_IN_USE,
	MAIN_INDEX_ONLY,
	REVERT_MAIN_INDEX_APPEND,
	REVERT_MAIN_INDEX_ONLY_APPEND
};

} // namespace duckdb
