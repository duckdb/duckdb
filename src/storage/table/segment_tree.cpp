#include "duckdb/storage/table/segment_tree.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

SegmentLock SegmentTree::Lock() {
	return SegmentLock(node_lock);
}

bool SegmentTree::IsEmpty(SegmentLock &) {
	return nodes.empty();
}

SegmentBase *SegmentTree::GetRootSegment(SegmentLock &l) {
	return nodes.empty() ? nullptr : nodes[0].node.get();
}

vector<SegmentNode> SegmentTree::MoveSegments(SegmentLock &) {
	return std::move(nodes);
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
		return nodes[index].node.get();
	} else {
		if (idx_t(index) >= nodes.size()) {
			return nullptr;
		}
		return nodes[index].node.get();
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
	return nodes.back().node.get();
}

SegmentBase *SegmentTree::GetLastSegment() {
	auto l = Lock();
	return GetLastSegment(l);
}

SegmentBase *SegmentTree::GetSegment(SegmentLock &l, idx_t row_number) {
	return nodes[GetSegmentIndex(l, row_number)].node.get();
}

SegmentBase *SegmentTree::GetSegment(idx_t row_number) {
	auto l = Lock();
	return GetSegment(l, row_number);
}

bool SegmentTree::TryGetSegmentIndex(SegmentLock &, idx_t row_number, idx_t &result) {
	if (nodes.empty()) {
		return false;
	}
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
			result = index;
			return true;
		}
	}
	return false;
}

idx_t SegmentTree::GetSegmentIndex(SegmentLock &l, idx_t row_number) {
	idx_t segment_index;
	if (TryGetSegmentIndex(l, row_number, segment_index)) {
		return segment_index;
	}
	string error;
	error = StringUtil::Format("Attempting to find row number \"%lld\" in %lld nodes\n", row_number, nodes.size());
	for (idx_t i = 0; i < nodes.size(); i++) {
		error +=
		    StringUtil::Format("Node %lld: Start %lld, Count %lld", i, nodes[i].row_start, nodes[i].node->count.load());
	}
	throw InternalException("Could not find node in column segment tree!\n%s%s", error, Exception::GetStackTrace());
}

idx_t SegmentTree::GetSegmentIndex(idx_t row_number) {
	auto l = Lock();
	return GetSegmentIndex(l, row_number);
}

bool SegmentTree::HasSegment(SegmentLock &, SegmentBase *segment) {
	for (auto &node : nodes) {
		if (node.node.get() == segment) {
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
	if (!nodes.empty()) {
		nodes.back().node->next = segment.get();
	}
	SegmentNode node;
	node.row_start = segment->start;
	node.node = std::move(segment);
	nodes.push_back(std::move(node));
}

void SegmentTree::AppendSegment(unique_ptr<SegmentBase> segment) {
	auto l = Lock();
	AppendSegment(l, std::move(segment));
}

void SegmentTree::EraseSegments(SegmentLock &, idx_t segment_start) {
	if (segment_start >= nodes.size() - 1) {
		return;
	}
	nodes.erase(nodes.begin() + segment_start + 1, nodes.end());
}

void SegmentTree::Replace(SegmentLock &, SegmentTree &other) {
	nodes = std::move(other.nodes);
}

void SegmentTree::Replace(SegmentTree &other) {
	auto l = Lock();
	Replace(l, other);
}

void SegmentTree::Verify(SegmentLock &) {
#ifdef DEBUG
	idx_t base_start = nodes.empty() ? 0 : nodes[0].node->start;
	for (idx_t i = 0; i < nodes.size(); i++) {
		D_ASSERT(nodes[i].row_start == nodes[i].node->start);
		D_ASSERT(nodes[i].node->start == base_start);
		base_start += nodes[i].node->count;
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
