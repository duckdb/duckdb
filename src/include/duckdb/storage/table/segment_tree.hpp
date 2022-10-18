//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/segment_tree.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/storage/table/segment_base.hpp"
#include "duckdb/storage/table/segment_lock.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {

struct SegmentNode {
	idx_t row_start;
	unique_ptr<SegmentBase> node;
};

//! The SegmentTree maintains a list of all segments of a specific column in a table, and allows searching for a segment
//! by row number
class SegmentTree {
public:
	//! Locks the segment tree. All methods to the segment tree either lock the segment tree, or take an already
	//! obtained lock.
	SegmentLock Lock();

	bool IsEmpty(SegmentLock &);

	//! Gets a pointer to the first segment. Useful for scans.
	SegmentBase *GetRootSegment();
	SegmentBase *GetRootSegment(SegmentLock &);
	//! Obtains ownership of the data of the segment tree
	vector<SegmentNode> MoveSegments(SegmentLock &);
	//! Gets a pointer to the nth segment. Negative numbers start from the back.
	SegmentBase *GetSegmentByIndex(int64_t index);
	SegmentBase *GetSegmentByIndex(SegmentLock &, int64_t index);

	//! Gets a pointer to the last segment. Useful for appends.
	SegmentBase *GetLastSegment();
	SegmentBase *GetLastSegment(SegmentLock &);
	//! Gets a pointer to a specific column segment for the given row
	SegmentBase *GetSegment(idx_t row_number);
	SegmentBase *GetSegment(SegmentLock &, idx_t row_number);

	//! Append a column segment to the tree
	void AppendSegment(unique_ptr<SegmentBase> segment);
	void AppendSegment(SegmentLock &, unique_ptr<SegmentBase> segment);
	//! Debug method, check whether the segment is in the segment tree
	bool HasSegment(SegmentBase *segment);
	bool HasSegment(SegmentLock &, SegmentBase *segment);

	//! Replace this tree with another tree, taking over its nodes in-place
	void Replace(SegmentTree &other);
	void Replace(SegmentLock &, SegmentTree &other);

	//! Erase all segments after a specific segment
	void EraseSegments(SegmentLock &, idx_t segment_start);

	//! Get the segment index of the column segment for the given row
	idx_t GetSegmentIndex(idx_t row_number);
	idx_t GetSegmentIndex(SegmentLock &, idx_t row_number);
	bool TryGetSegmentIndex(SegmentLock &, idx_t row_number, idx_t &);

	void Verify(SegmentLock &);
	void Verify();

private:
	//! The nodes in the tree, can be binary searched
	vector<SegmentNode> nodes;
	//! Lock to access or modify the nodes
	mutex node_lock;
};

} // namespace duckdb
