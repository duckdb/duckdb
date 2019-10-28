//===----------------------------------------------------------------------===//
//                         DuckDB
//
// transaction/update_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"

namespace duckdb {
class DataTable;
class UncompressedSegment;

struct UpdateInfo {
	//! The table that this update info affects
	DataTable *table;
	//! The uncompressed segment that this update info affects
	UncompressedSegment *segment;
	//! The version number
	transaction_t version_number;
	//! The vector index within the uncompressed segment
	index_t vector_index;
	//! The amount of updated tuples
	sel_t N;
	//! The maximum amount of tuples that can fit into this UpdateInfo
	sel_t max;
	//! The row ids of the tuples that have been updated. This should always be kept sorted!
	sel_t *tuples;
	//! The nullmask of the tuples
	nullmask_t nullmask;
	//! The data of the tuples
	data_ptr_t tuple_data;
	//! The previous update info (or nullptr if it is the base)
	UpdateInfo *prev;
	//! The next update info in the chain (or nullptr if it is the last)
	UpdateInfo *next;
};

}
