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
	//! Remove from main index, insert into deleted_rows_in_use
	MAIN_INDEX,
	//! Remove from main index only
	MAIN_INDEX_ONLY,
	//! Revert MAIN_INDEX, i.e. append to main index and remove from deleted_rows_in_use
	REVERT_MAIN_INDEX,
	//! Revert MAIN_INDEX_ONLY, i.e. append to main index
	REVERT_MAIN_INDEX_ONLY,
	//! Remove from deleted_rows_in_use
	DELETED_ROWS_IN_USE
};

} // namespace duckdb
