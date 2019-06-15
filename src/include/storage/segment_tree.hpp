//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/segment_tree.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"
#include "storage/storage_lock.hpp"

#include <mutex>

namespace duckdb {

class SegmentBase {
public:
	SegmentBase(index_t start, index_t count) : start(start), count(count) {
	}
	virtual ~SegmentBase() {
	}

	//! The start row id of this chunk
	index_t start;
	//! The amount of entries in this storage chunk
	index_t count;
	//! The next segment after this one
	unique_ptr<SegmentBase> next;
};

struct SegmentNode {
	index_t row_start;
	SegmentBase *node;
};

//! The SegmentTree maintains a list of all segments of a specific column in a table, and allows searching for a segment
//! by row number
class SegmentTree {
public:
	//! The initial segment of the tree
	unique_ptr<SegmentBase> root_node;
	//! The nodes in the tree, can be binary searched
	vector<SegmentNode> nodes;
	//! Lock to access or modify the nodes
	std::mutex node_lock;

public:
	//! Gets a pointer to the first segment. Useful for scans.
	SegmentBase *GetRootSegment();
	//! Gets a pointer to the last segment. Useful for appends.
	SegmentBase *GetLastSegment();
	//! Gets a pointer to a specific column segment for the given row
	SegmentBase *GetSegment(index_t row_number);
	//! Append a column segment to the tree
	void AppendSegment(unique_ptr<SegmentBase> segment);
};

} // namespace duckdb
