//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/numeric_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/uncompressed_segment.hpp"

namespace duckdb {

class NumericSegment : public UncompressedSegment {
public:
	NumericSegment(BufferManager &manager, TypeId type, idx_t row_start, block_id_t block_id = INVALID_BLOCK);

	//! The size of this type
	idx_t type_size;

public:
	//! Fetch a single value and append it to the vector
	void FetchRow(ColumnFetchState &state, Transaction &transaction, row_t row_id, Vector &result,
	              idx_t result_idx) override;

	//! Append a part of a vector to the uncompressed segment with the given append state, updating the provided stats
	//! in the process. Returns the amount of tuples appended. If this is less than `count`, the uncompressed segment is
	//! full.
	idx_t Append(SegmentStatistics &stats, Vector &data, idx_t offset, idx_t count) override;

	//! Rollback a previous update
	void RollbackUpdate(UpdateInfo *info) override;

protected:
	void Update(ColumnData &data, SegmentStatistics &stats, Transaction &transaction, Vector &update, row_t *ids,
	            idx_t count, idx_t vector_index, idx_t vector_offset, UpdateInfo *node) override;
	void Select(ColumnScanState &state, Vector &result, SelectionVector &sel, idx_t &approved_tuple_count,
	            vector<TableFilter> &tableFilter) override;
	void FetchBaseData(ColumnScanState &state, idx_t vector_index, Vector &result) override;
	void FilterFetchBaseData(ColumnScanState &state, Vector &result, SelectionVector &sel,
	                         idx_t &approved_tuple_count) override;
	void FetchUpdateData(ColumnScanState &state, Transaction &transaction, UpdateInfo *versions,
	                     Vector &result) override;

public:
	typedef void (*append_function_t)(SegmentStatistics &stats, data_ptr_t target, idx_t target_offset, Vector &source,
	                                  idx_t offset, idx_t count);
	typedef void (*update_function_t)(SegmentStatistics &stats, UpdateInfo *info, data_ptr_t base_data, Vector &update);
	typedef void (*update_info_fetch_function_t)(Transaction &transaction, UpdateInfo *info, Vector &result);
	typedef void (*update_info_append_function_t)(Transaction &transaction, UpdateInfo *info, idx_t idx, Vector &result,
	                                              idx_t result_idx);
	typedef void (*rollback_update_function_t)(UpdateInfo *info, data_ptr_t base_data);
	typedef void (*merge_update_function_t)(SegmentStatistics &stats, UpdateInfo *node, data_ptr_t target,
	                                        Vector &update, row_t *ids, idx_t count, idx_t vector_offset);

private:
	append_function_t append_function;
	update_function_t update_function;
	update_info_fetch_function_t fetch_from_update_info;
	update_info_append_function_t append_from_update_info;
	rollback_update_function_t rollback_update;
	merge_update_function_t merge_update_function;
};

template <class F1, class F2, class F3>
static idx_t merge_loop(row_t a[], sel_t b[], idx_t acount, idx_t bcount, idx_t aoffset, F1 merge, F2 pick_a,
                        F3 pick_b) {
	idx_t aidx = 0, bidx = 0;
	idx_t count = 0;
	while (aidx < acount && bidx < bcount) {
		auto a_id = a[aidx] - aoffset;
		auto b_id = b[bidx];
		if (a_id == b_id) {
			merge(a_id, aidx, bidx, count);
			aidx++;
			bidx++;
			count++;
		} else if (a_id < b_id) {
			pick_a(a_id, aidx, count);
			aidx++;
			count++;
		} else {
			pick_b(b_id, bidx, count);
			bidx++;
			count++;
		}
	}
	for (; aidx < acount; aidx++) {
		pick_a(a[aidx] - aoffset, aidx, count);
		count++;
	}
	for (; bidx < bcount; bidx++) {
		pick_b(b[bidx], bidx, count);
		count++;
	}
	return count;
}

} // namespace duckdb
