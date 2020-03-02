//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/update_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {
class ColumnData;
class UncompressedSegment;

struct UpdateInfo {
	//! The base ColumnData that this update affects
	ColumnData *column_data;
	//! The uncompressed segment that this update info affects
	UncompressedSegment *segment;
	//! The version number
	transaction_t version_number;
	//! The vector index within the uncompressed segment
	idx_t vector_index;
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

	//! Loop over the update chain and execute the specified callback on all UpdateInfo's that are relevant for that
	//! transaction in-order of newest to oldest
	template <class T> static void UpdatesForTransaction(UpdateInfo *current, Transaction &transaction, T &&callback) {
		while (current) {
			if (current->version_number > transaction.start_time &&
			    current->version_number != transaction.transaction_id) {
				// these tuples were either committed AFTER this transaction started or are not committed yet, use
				// tuples stored in this version
				callback(current);
			}
			current = current->next;
		}
	}
};

} // namespace duckdb
