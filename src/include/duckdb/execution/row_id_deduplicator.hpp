//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/row_id_deduplicator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"

namespace duckdb {

//! Registers the first `count` row-ids of `row_ids` into `seen`, handling any vector layout. When `sel` is
//! provided, records the input position of each first-seen row-id (keep-first deduplication). Returns the number
//! of distinct (first-seen) row-ids. The caller owns its locking and duplicate policy: keep-first by slicing with
//! `sel`, or raise its own error when the returned count is less than `count`. Shared by UPDATE, MERGE, and
//! ON CONFLICT DO UPDATE, which otherwise duplicated this row-id iteration.
inline idx_t RegisterRowIds(unordered_set<row_t> &seen, const Vector &row_ids, idx_t count,
                            optional_ptr<SelectionVector> sel = nullptr) {
	idx_t distinct_count = 0;
	for (const auto &entry : row_ids.Values<row_t>()) {
		// the caller may pass fewer meaningful entries than the vector holds (e.g. conflict row-ids)
		if (entry.GetIndex() >= count) {
			break;
		}
		if (seen.insert(entry.GetValue()).second) {
			if (sel) {
				sel->set_index(distinct_count, entry.GetIndex());
			}
			distinct_count++;
		}
	}
	return distinct_count;
}

} // namespace duckdb
