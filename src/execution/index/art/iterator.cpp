#include "duckdb/execution/index/art/iterator.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/execution/index/art/prefix.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// IteratorKey
//===--------------------------------------------------------------------===//

bool IteratorKey::Contains(const ARTKey &key) const {
	if (Size() < key.len) {
		return false;
	}
	for (idx_t i = 0; i < key.len; i++) {
		if (key_bytes[i] != key.data[i]) {
			return false;
		}
	}
	return true;
}

bool IteratorKey::GreaterThan(const ARTKey &key, const bool equal, const uint8_t nested_depth) const {
	for (idx_t i = 0; i < MinValue<idx_t>(Size(), key.len); i++) {
		if (key_bytes[i] > key.data[i]) {
			return true;
		} else if (key_bytes[i] < key.data[i]) {
			return false;
		}
	}

	// Returns true, if current_key is greater than (or equal to) key.
	D_ASSERT(Size() >= nested_depth);
	auto this_len = Size() - nested_depth;
	return equal ? this_len > key.len : this_len >= key.len;
}

//===--------------------------------------------------------------------===//
// Iterator
//===--------------------------------------------------------------------===//

bool Iterator::Scan(const ARTKey &upper_bound, const idx_t max_count, set<row_t> &row_ids, const bool equal) {
	bool has_next;
	do {
		// An empty upper bound indicates that no upper bound exists.
		if (!upper_bound.Empty()) {
			if (status == GateStatus::GATE_NOT_SET || entered_nested_leaf) {
				if (current_key.GreaterThan(upper_bound, equal, nested_depth)) {
					return true;
				}
			}
		}

		switch (last_leaf.GetType()) {
		case NType::LEAF_INLINED:
			if (row_ids.size() + 1 > max_count) {
				return false;
			}
			row_ids.insert(last_leaf.GetRowId());
			break;
		case NType::LEAF:
			if (!Leaf::DeprecatedGetRowIds(art, last_leaf, row_ids, max_count)) {
				return false;
			}
			break;
		case NType::NODE_7_LEAF:
		case NType::NODE_15_LEAF:
		case NType::NODE_256_LEAF: {
			uint8_t byte = 0;
			while (last_leaf.GetNextByte(art, byte)) {
				if (row_ids.size() + 1 > max_count) {
					return false;
				}
				row_id[ROW_ID_SIZE - 1] = byte;
				ARTKey key(&row_id[0], ROW_ID_SIZE);
				row_ids.insert(key.GetRowId());
				if (byte == NumericLimits<uint8_t>::Maximum()) {
					break;
				}
				byte++;
			}
			break;
		}
		default:
			throw InternalException("Invalid leaf type for index scan.");
		}

		entered_nested_leaf = false;
		has_next = Next();
	} while (has_next);
	return true;
}

void Iterator::FindMinimum(const Node &node) {
	reference<const Node> ref(node);

	while (ref.get().HasMetadata()) {
		// Found the minimum.
		if (ref.get().IsAnyLeaf()) {
			last_leaf = ref.get();
			return;
		}

		// We are passing a gate node.
		if (ref.get().GetGateStatus() == GateStatus::GATE_SET) {
			D_ASSERT(status == GateStatus::GATE_NOT_SET);
			status = GateStatus::GATE_SET;
			entered_nested_leaf = true;
			nested_depth = 0;
		}

		// Traverse the prefix.
		if (ref.get().GetType() == NType::PREFIX) {
			Prefix prefix(art, ref.get());
			for (idx_t i = 0; i < prefix.data[Prefix::Count(art)]; i++) {
				current_key.Push(prefix.data[i]);
				if (status == GateStatus::GATE_SET) {
					row_id[nested_depth] = prefix.data[i];
					nested_depth++;
					D_ASSERT(nested_depth < Prefix::ROW_ID_SIZE);
				}
			}
			nodes.emplace(ref.get(), 0);
			ref = *prefix.ptr;
			continue;
		}

		// Go to the leftmost entry in the current node.
		uint8_t byte = 0;
		auto next = ref.get().GetNextChild(art, byte);
		D_ASSERT(next);

		// Move to the leftmost node.
		current_key.Push(byte);
		if (status == GateStatus::GATE_SET) {
			row_id[nested_depth] = byte;
			nested_depth++;
			D_ASSERT(nested_depth < Prefix::ROW_ID_SIZE);
		}
		nodes.emplace(ref.get(), byte);
		ref = *next;
	}
	// Should always have a node with metadata.
	throw InternalException("ART Iterator::FindMinimum: Reached node without metadata");
}

bool Iterator::LowerBound(const Node &node, const ARTKey &key, const bool equal) {
	reference<const Node> ref(node);
	idx_t depth = 0;

	while (ref.get().HasMetadata()) {
		// We found any leaf node, or a gate.
		if (ref.get().IsAnyLeaf() || ref.get().GetGateStatus() == GateStatus::GATE_SET) {
			D_ASSERT(status == GateStatus::GATE_NOT_SET);
			D_ASSERT(current_key.Size() == key.len);
			if (!equal && current_key.Contains(key)) {
				return Next();
			}

			if (ref.get().GetGateStatus() == GateStatus::GATE_SET) {
				FindMinimum(ref.get());
			} else {
				last_leaf = ref.get();
			}
			return true;
		}

		D_ASSERT(ref.get().GetGateStatus() == GateStatus::GATE_NOT_SET);
		if (ref.get().GetType() != NType::PREFIX) {
			auto next_byte = key[depth];
			auto child = ref.get().GetNextChild(art, next_byte);

			// The key is greater than any key in this subtree.
			if (!child) {
				return Next();
			}

			current_key.Push(next_byte);
			nodes.emplace(ref.get(), next_byte);

			// We return the minimum because all keys are greater than the lower bound.
			if (next_byte > key[depth]) {
				FindMinimum(*child);
				return true;
			}

			// Move to the child and increment depth.
			ref = *child;
			depth++;
			continue;
		}

		// Push back all prefix bytes.
		Prefix prefix(art, ref.get());
		for (idx_t i = 0; i < prefix.data[Prefix::Count(art)]; i++) {
			current_key.Push(prefix.data[i]);
		}
		nodes.emplace(ref.get(), 0);

		// We compare the prefix bytes with the key bytes.
		for (idx_t i = 0; i < prefix.data[Prefix::Count(art)]; i++) {
			// We found a prefix byte that is less than its corresponding key byte.
			// I.e., the subsequent node is lesser than the key. Thus, the next node
			// is the lower bound.
			if (prefix.data[i] < key[depth + i]) {
				return Next();
			}

			// We found a prefix byte that is greater than its corresponding key byte.
			// I.e., the subsequent node is greater than the key. Thus, the minimum is
			// the lower bound.
			if (prefix.data[i] > key[depth + i]) {
				FindMinimum(*prefix.ptr);
				return true;
			}
		}

		// The prefix matches the key. Move to the child and update depth.
		depth += prefix.data[Prefix::Count(art)];
		ref = *prefix.ptr;
	}
	// Should always have a node with metadata.
	throw InternalException("ART Iterator::LowerBound: Reached node without metadata");
}

bool Iterator::Next() {
	while (!nodes.empty()) {
		auto &top = nodes.top();
		D_ASSERT(!top.node.IsAnyLeaf());

		if (top.node.GetType() == NType::PREFIX) {
			PopNode();
			continue;
		}

		if (top.byte == NumericLimits<uint8_t>::Maximum()) {
			// No more children of this node.
			// Move up the tree by popping the key byte of the current node.
			PopNode();
			continue;
		}

		top.byte++;
		auto next_node = top.node.GetNextChild(art, top.byte);
		if (!next_node) {
			// No more children of this node.
			// Move up the tree by popping the key byte of the current node.
			PopNode();
			continue;
		}

		current_key.Pop(1);
		current_key.Push(top.byte);
		if (status == GateStatus::GATE_SET) {
			row_id[nested_depth - 1] = top.byte;
		}

		FindMinimum(*next_node);
		return true;
	}
	return false;
}

void Iterator::PopNode() {
	auto gate_status = nodes.top().node.GetGateStatus();

	// Pop the byte and the node.
	if (nodes.top().node.GetType() != NType::PREFIX) {
		current_key.Pop(1);
		if (status == GateStatus::GATE_SET) {
			nested_depth--;
			D_ASSERT(nested_depth < Prefix::ROW_ID_SIZE);
		}

	} else {
		// Pop all prefix bytes and the node.
		Prefix prefix(art, nodes.top().node);
		auto prefix_byte_count = prefix.data[Prefix::Count(art)];
		current_key.Pop(prefix_byte_count);

		if (status == GateStatus::GATE_SET) {
			nested_depth -= prefix_byte_count;
			D_ASSERT(nested_depth < Prefix::ROW_ID_SIZE);
		}
	}
	nodes.pop();

	// We are popping a gate node.
	if (gate_status == GateStatus::GATE_SET) {
		D_ASSERT(status == GateStatus::GATE_SET);
		status = GateStatus::GATE_NOT_SET;
	}
}

} // namespace duckdb
