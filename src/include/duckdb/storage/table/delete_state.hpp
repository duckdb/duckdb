//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/delete_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/append_state.hpp"

namespace duckdb {
class TableCatalogEntry;

struct TableDeleteState {
	unique_ptr<ConstraintState> constraint_state;
	bool has_delete_constraints = false;
	DataChunk verify_chunk;
	vector<StorageIndex> col_ids;
	shared_ptr<CheckpointLock> checkpoint_lock;
	//! True if this delete is the DELETE half of an UPDATE rewrite (UPDATE/INSERT OR REPLACE/
	//! ON CONFLICT DO UPDATE) where the updated columns don't intersect any FK's PK columns and
	//! the FK is not self-referential. In that case the matching INSERT half re-adds the same
	//! PK in the same statement, so the delete-side FK check is spurious and can be skipped.
	bool skip_unchanged_fk_delete_check = false;
};

} // namespace duckdb
