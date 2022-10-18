#include "duckdb/storage/table/segment_tree.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

SegmentLock SegmentTree::Lock() {
	return SegmentLock(node_lock);
}

bool SegmentTree::IsEmpty(SegmentLock &) {
	D_ASSERT(nodes.empty() == (root_node.get() == nullptr));
	return nodes.empty();
}

SegmentBase *SegmentTree::GetRootSegment(SegmentLock &l) {
	Verify(l);
	return root_node.get();
}

unique_ptr<SegmentBase> SegmentTree::GrabRootSegment(SegmentLock &) {
	return move(root_node);
}

SegmentBase *SegmentTree::GetRootSegment() {
	auto l = Lock();
	return GetRootSegment(l);
}

SegmentBase *SegmentTree::GetSegmentByIndex(SegmentLock &, int64_t index) {
	if (index < 0) {
		index = nodes.size() + index;
		if (index < 0) {
			return nullptr;
		}
		return nodes[index].node;
	} else {
		if (idx_t(index) >= nodes.size()) {
			return nullptr;
		}
		return nodes[index].node;
	}
}
SegmentBase *SegmentTree::GetSegmentByIndex(int64_t index) {
	auto l = Lock();
	return GetSegmentByIndex(l, index);
}

SegmentBase *SegmentTree::GetLastSegment(SegmentLock &l) {
	if (nodes.empty()) {
		return nullptr;
	}
	Verify(l);
	return nodes.back().node;
}

SegmentBase *SegmentTree::GetLastSegment() {
	auto l = Lock();
	return GetLastSegment(l);
}

SegmentBase *SegmentTree::GetSegment(SegmentLock &l, idx_t row_number) {
	return nodes[GetSegmentIndex(l, row_number)].node;
}

SegmentBase *SegmentTree::GetSegment(idx_t row_number) {
	auto l = Lock();
	return GetSegment(l, row_number);
}

idx_t SegmentTree::GetSegmentIndex(SegmentLock &, idx_t row_number) {
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

idx_t SegmentTree::GetSegmentIndex(idx_t row_number) {
	auto l = Lock();
	return GetSegmentIndex(l, row_number);
}

bool SegmentTree::HasSegment(SegmentLock &, SegmentBase *segment) {
	for (auto &node : nodes) {
		if (node.node == segment) {
			return true;
		}
	}
	return false;
}

bool SegmentTree::HasSegment(SegmentBase *segment) {
	auto l = Lock();
	return HasSegment(l, segment);
}

void SegmentTree::AppendSegment(SegmentLock &, unique_ptr<SegmentBase> segment) {
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

void SegmentTree::AppendSegment(unique_ptr<SegmentBase> segment) {
	auto l = Lock();
	AppendSegment(l, move(segment));
}

void SegmentTree::EraseSegments(SegmentLock &, idx_t segment_start) {
	if (segment_start >= nodes.size() - 1) {
		return;
	}
	nodes.erase(nodes.begin() + segment_start + 1, nodes.end());
}

void SegmentTree::Replace(SegmentLock &, SegmentTree &other) {
	root_node = move(other.root_node);
	nodes = move(other.nodes);
}

void SegmentTree::Replace(SegmentTree &other) {
	auto l = Lock();
	Replace(l, other);
}

void SegmentTree::Verify(SegmentLock &) {
#ifdef DEBUG
	auto segment = root_node.get();
	if (!segment) {
		D_ASSERT(nodes.empty());
		return;
	}
	idx_t segment_idx = 0;
	idx_t base_start = segment->start;
	while (segment) {
		D_ASSERT(segment_idx < nodes.size());
		D_ASSERT(nodes[segment_idx].node == segment);
		D_ASSERT(nodes[segment_idx].row_start == segment->start);
		D_ASSERT(segment->start == base_start);
		base_start += segment->count;
		segment = segment->next.get();
		segment_idx++;
	}
#endif
}

void SegmentTree::Verify() {
#ifdef DEBUG
	auto l = Lock();
	Verify(l);
#endif
}

} // namespace duckdb
