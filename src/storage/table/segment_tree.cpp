#include "duckdb/storage/table/segment_tree.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
namespace duckdb {

SegmentBase *SegmentTree::GetRootSegment() {
	return root_node.get();
}

SegmentBase *SegmentTree::GetLastSegment() {
	return nodes.empty() ? nullptr : nodes.back().node;
}

SegmentBase *SegmentTree::GetSegment(idx_t row_number) {
	lock_guard<mutex> tree_lock(node_lock);
	return nodes[GetSegmentIndex(row_number)].node;
}

idx_t SegmentTree::GetSegmentIndex(idx_t row_number) {
	D_ASSERT(!nodes.empty());
	D_ASSERT(row_number >= nodes[0].row_start);
	D_ASSERT(row_number < nodes.back().row_start + nodes.back().node->count);
	idx_t lower = 0;
	idx_t upper = nodes.size() - 1;
	// binary search to find the node
	while (lower <= upper) {
		idx_t index = (lower + upper) / 2;
		D_ASSERT(index < nodes.size());
		auto &entry = nodes[index];
		D_ASSERT(entry.row_start == entry.node->start);
		if (row_number < entry.row_start) {
			upper = index - 1;
		} else if (row_number >= entry.row_start + entry.node->count) {
			lower = index + 1;
		} else {
			return index;
		}
	}
	throw InternalException("Could not find node in column segment tree!");
}

void SegmentTree::AppendSegment(unique_ptr<SegmentBase> segment) {
	D_ASSERT(segment);
	// add the node to the list of nodes
	SegmentNode node;
	node.row_start = segment->start;
	node.node = segment.get();
	nodes.push_back(node);

	if (nodes.size() > 1) {
		// add the node as the next pointer of the last node
		D_ASSERT(!nodes[nodes.size() - 2].node->next);
		nodes[nodes.size() - 2].node->next = move(segment);
	} else {
		root_node = move(segment);
	}
}

void SegmentTree::Replace(SegmentTree &other) {
	root_node = move(other.root_node);
	nodes = move(other.nodes);
}

} // namespace duckdb
