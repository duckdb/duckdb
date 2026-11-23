//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/index_build.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

//! Consecutive rows of a table that a commit changed during an index build
struct IndexBuildRange {
	idx_t start_row;
	idx_t count;

	idx_t End() const {
		return start_row + count;
	}
};

//! The changes that commits made during an index build
struct IndexBuildChanges {
	//! The changed row ranges, sorted and merged
	vector<IndexBuildRange> ranges;
	//! The number of rows the commits changed, which can be fewer than the ranges cover
	idx_t changed_rows = 0;
};

//! Row ranges that commits change during an index build, applied before the index is added
class IndexBuild {
public:
	IndexBuild(transaction_t transaction_id, transaction_t build_commit);

	//! The transaction that builds the index
	const transaction_t transaction_id;
	//! The last commit before the build, which the build scans as of
	const transaction_t build_commit;

public:
	//! The last commit whose changes the built index holds
	transaction_t GetAppliedCommit() const {
		return applied_commit;
	}

	//! Adds rows that a commit appended or deleted, changed_rows of them in the range
	void AddChanges(const IndexBuildRange &rows, idx_t changed_rows);
	//! Takes the changes that the commits made since the last call
	IndexBuildChanges TakeChanges();
	//! Marks the changes up to and including this commit as applied
	void SetAppliedCommit(transaction_t commit_id);
	//! Sorts the ranges by start row and merges overlapping and adjacent ones
	static void MergeRanges(vector<IndexBuildRange> &ranges);

private:
	//! The last commit whose changes the built index holds
	transaction_t applied_commit;
	//! The changes that the build has not taken yet, guarded by the commit lock
	IndexBuildChanges changes;
};

} // namespace duckdb
