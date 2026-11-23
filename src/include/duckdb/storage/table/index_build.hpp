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

	//! Adds consecutive rows that a commit appended or deleted
	void AddChanges(const IndexBuildRange &rows);
	//! Takes the rows that the commits changed since the last call, merged
	vector<IndexBuildRange> TakeChanges();
	//! Marks the changes up to and including this commit as applied
	void SetAppliedCommit(transaction_t commit_id);
	//! Sorts the ranges by start row and merges overlapping and adjacent ones
	static void MergeRanges(vector<IndexBuildRange> &ranges);

private:
	//! The last commit whose changes the built index holds
	transaction_t applied_commit;
	//! The changes that the build has not taken yet, guarded by the commit lock
	vector<IndexBuildRange> changes;
};

} // namespace duckdb
