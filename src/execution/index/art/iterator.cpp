#include "duckdb/execution/index/art/iterator.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/execution/index/art/node_handle.hpp"
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

template <typename Output>
ARTScanResult Iterator::Scan(const ARTKey &upper_bound, Output &output, bool equal) {
	bool has_next;
	do {
		// An empty upper bound indicates that no upper bound exists.
		if (!upper_bound.Empty()) {
			if (status == GateStatus::GATE_NOT_SET || entered_nested_leaf) {
				if (current_key.GreaterThan(upper_bound, equal, nested_depth)) {
					return ARTScanResult::COMPLETED;
				}
			}
		}

		// Set the current key in the output policy.
		D_ASSERT(current_key.Size() >= nested_depth);
		auto key_len = current_key.Size() - nested_depth;
		output.SetKey(current_key, key_len);

		switch (last_leaf.GetType()) {
		case NType::LEAF_INLINED: {
			if (output.IsFull()) {
				return ARTScanResult::PAUSED;
			}
			output.Add(last_leaf.GetRowId());
			break;
		}
		case NType::LEAF: {
			D_ASSERT(nested_depth == 0);
			if (!resume_state.has_cached_row_ids) {
				resume_state.cached_row_ids.clear();
				Leaf::DeprecatedGetRowIds(art, last_leaf, resume_state.cached_row_ids, NumericLimits<idx_t>::Maximum());
				resume_state.cached_row_ids_it = resume_state.cached_row_ids.begin();
				resume_state.has_cached_row_ids = true;
			}
			// Try to output the next entry in the deprecated leaf chain.
			while (resume_state.cached_row_ids_it != resume_state.cached_row_ids.end()) {
				if (output.IsFull()) {
					// If we pause here, then scanning will resume at cached_row_ids_it.
					return ARTScanResult::PAUSED;
				}
				output.Add(*resume_state.cached_row_ids_it);
				++resume_state.cached_row_ids_it;
			}
			resume_state.has_cached_row_ids = false;
			break;
		}
		case NType::NODE_7_LEAF:
		case NType::NODE_15_LEAF:
		case NType::NODE_256_LEAF: {
			// If we haven't traversed this leaf yet, set nested_started to true (allows us to pick up iteration again
			// in case we fill the output with capacity.
			if (!resume_state.nested_started) {
				resume_state.nested_byte = 0;
				resume_state.nested_started = true;
			}
			// Try to output the next inlined leaf.
			while (last_leaf.GetNextByte(art, resume_state.nested_byte)) {
				if (output.IsFull()) {
					// If we pause here, then scanning will resume at nested_byte in the current leaf.
					return ARTScanResult::PAUSED;
				}
				row_id[ROW_ID_SIZE - 1] = resume_state.nested_byte;
				ARTKey rid_key(&row_id[0], ROW_ID_SIZE);
				output.Add(rid_key.GetRowId());

				if (resume_state.nested_byte == NumericLimits<uint8_t>::Maximum()) {
					break;
				}
				resume_state.nested_byte++;
			}
			resume_state.nested_started = false;
			break;
		}
		default:
			throw InternalException("Invalid leaf type for index scan.");
		}

		entered_nested_leaf = false;
		has_next = Next();
	} while (has_next);
	return ARTScanResult::COMPLETED;
}

// Explicit template instantiations for the two output policies.
template ARTScanResult Iterator::Scan<RowIdSetOutput>(const ARTKey &, RowIdSetOutput &, bool);
template ARTScanResult Iterator::Scan<KeyRowIdOutput>(const ARTKey &, KeyRowIdOutput &, bool);

void Iterator::FindMinimum(Node node) {
	while (node.HasMetadata()) {
		// Found the minimum.
		if (node.IsAnyLeaf()) {
			last_leaf = node;
			return;
		}

		// We are passing a gate node.
		if (node.GetGateStatus() == GateStatus::GATE_SET) {
			D_ASSERT(status == GateStatus::GATE_NOT_SET);
			status = GateStatus::GATE_SET;
			entered_nested_leaf = true;
			nested_depth = 0;
		}

		// Traverse the prefix.
		if (node.GetType() == NType::PREFIX) {
			Node child;
			{
				ConstNodeHandle handle(art, node);
				auto data = handle.GetPtr();
				auto count = data[art.PrefixCount()];
				auto child_ptr = reinterpret_cast<const Node *>(data + art.PrefixCount() + 1);

				for (idx_t i = 0; i < count; i++) {
					current_key.Push(data[i]);
					if (status == GateStatus::GATE_SET) {
						row_id[nested_depth] = data[i];
						nested_depth++;
						D_ASSERT(nested_depth < Prefix::ROW_ID_SIZE);
					}
				}
				child = *child_ptr;
			}
			nodes.emplace(node, 0);
			node = child;
			continue;
		}

		// Go to the leftmost entry in the current node.
		uint8_t byte = 0;
		auto child = node.GetNextChildNode(art, byte);
		D_ASSERT(child.HasMetadata());

		// Move to the leftmost node.
		current_key.Push(byte);
		if (status == GateStatus::GATE_SET) {
			row_id[nested_depth] = byte;
			nested_depth++;
			D_ASSERT(nested_depth < Prefix::ROW_ID_SIZE);
		}
		nodes.emplace(node, byte);
		node = child;
	}
	// Should always have a node with metadata.
	throw InternalException("ART Iterator::FindMinimum: Reached node without metadata");
}

bool Iterator::LowerBound(Node node, const ARTKey &key, const bool equal) {
	idx_t depth = 0;

	while (node.HasMetadata()) {
		// We found any leaf node, or a gate.
		if (node.IsAnyLeaf() || node.GetGateStatus() == GateStatus::GATE_SET) {
			D_ASSERT(status == GateStatus::GATE_NOT_SET);
			D_ASSERT(current_key.Size() == key.len);
			if (!equal && current_key.Contains(key)) {
				return Next();
			}

			if (node.GetGateStatus() == GateStatus::GATE_SET) {
				FindMinimum(node);
			} else {
				last_leaf = node;
			}
			return true;
		}

		D_ASSERT(node.GetGateStatus() == GateStatus::GATE_NOT_SET);
		if (node.GetType() != NType::PREFIX) {
			auto next_byte = key[depth];
			auto child = node.GetNextChildNode(art, next_byte);

			// The key is greater than any key in this subtree.
			if (!child.HasMetadata()) {
				return Next();
			}

			current_key.Push(next_byte);
			nodes.emplace(node, next_byte);

			// We return the minimum because all keys are greater than the lower bound.
			if (next_byte > key[depth]) {
				FindMinimum(child);
				return true;
			}

			// Move to the child and increment depth.
			node = child;
			depth++;
			continue;
		}

		// Push back all prefix bytes and compare with key bytes.
		uint8_t prefix_count;
		Node prefix_child;
		{
			ConstNodeHandle handle(art, node);
			auto data = handle.GetPtr();
			prefix_count = data[art.PrefixCount()];
			auto child_ptr = reinterpret_cast<const Node *>(data + art.PrefixCount() + 1);

			for (idx_t i = 0; i < prefix_count; i++) {
				current_key.Push(data[i]);
			}
			nodes.emplace(node, 0);

			// We compare the prefix bytes with the key bytes.
			for (idx_t i = 0; i < prefix_count; i++) {
				// We found a prefix byte that is less than its corresponding key byte.
				// I.e., the subsequent node is lesser than the key. Thus, the next node
				// is the lower bound.
				if (data[i] < key[depth + i]) {
					return Next();
				}

				// We found a prefix byte that is greater than its corresponding key byte.
				// I.e., the subsequent node is greater than the key. Thus, the minimum is
				// the lower bound.
				if (data[i] > key[depth + i]) {
					prefix_child = *child_ptr;
					FindMinimum(prefix_child);
					return true;
				}
			}

			// The prefix matches the key.
			prefix_child = *child_ptr;
		}
		depth += prefix_count;
		node = prefix_child;
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
		auto child = top.node.GetNextChildNode(art, top.byte);
		if (!child.HasMetadata()) {
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

		FindMinimum(child);
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
		ConstNodeHandle handle(art, nodes.top().node);
		auto data = handle.GetPtr();
		auto prefix_byte_count = data[art.PrefixCount()];
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
