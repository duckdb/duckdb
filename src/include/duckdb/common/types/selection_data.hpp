//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/selection_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/allocator.hpp"

namespace duckdb {

//! Selection indices whose lifetime is managed by buffer_ptr.
struct SelectionData {
	DUCKDB_API explicit SelectionData(idx_t count);

	AllocatedData owned_data;
};

} // namespace duckdb
