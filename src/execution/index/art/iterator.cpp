#include "duckdb/execution/index/art/iterator.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/execution/index/art/prefix.hpp"

namespace duckdb {

bool IteratorKey::operator>(const ARTKey &k) const {
	for (idx_t i = 0; i < MinValue<idx_t>(key_bytes.size(), k.len); i++) {
		if (key_bytes[i] > k.data[i]) {
			return true;
		} else if (key_bytes[i] < k.data[i]) {
			return false;
		}
	}
	return key_bytes.size() > k.len;
}

bool IteratorKey::operator>=(const ARTKey &k) const {
	for (idx_t i = 0; i < MinValue<idx_t>(key_bytes.size(), k.len); i++) {
		if (key_bytes[i] > k.data[i]) {
			return true;
		} else if (key_bytes[i] < k.data[i]) {
			return false;
		}
	}
	return key_bytes.size() >= k.len;
}

bool IteratorKey::operator==(const ARTKey &k) const {
	// NOTE: we only use this for finding the LowerBound, in which case the length
	// has to be equal
	D_ASSERT(key_bytes.size() == k.len);
	for (idx_t i = 0; i < key_bytes.size(); i++) {
		if (key_bytes[i] != k.data[i]) {
			return false;
		}
	}
	return true;
}

bool Iterator::Scan(const ARTKey &upper_bound, const idx_t max_count, vector<row_t> &result_ids, const bool equal) {

	bool has_next;
	do {
		if (!upper_bound.Empty()) {
			// no more row IDs within the key bounds
			if (equal) {
				if (current_key > upper_bound) {
					return true;
				}
			} else {
				if (current_key >= upper_bound) {
					return true;
				}
			}
		}

		// adding more elements would exceed the maximum count
		if (result_ids.size() + last_leaf->count > max_count) {
			return false;
		}

		// FIXME: copy all at once to improve performance
		for (idx_t i = 0; i < last_leaf->count; i++) {
			row_t row_id = last_leaf->GetRowId(*art, i);
			result_ids.push_back(row_id);
		}

		// get the next leaf
		has_next = Next();

	} while (has_next);

	return true;
}

void Iterator::FindMinimum(Node &node) {

	D_ASSERT(node.IsSet());
	if (node.IsSwizzled()) {
		node.Deserialize(*art);
	}

	// found the minimum
	if (node.DecodeARTNodeType() == NType::LEAF) {
		last_leaf = Node::GetAllocator(*art, NType::LEAF).Get<Leaf>(node);
		return;
	}

	// traverse the prefix
	if (node.DecodeARTNodeType() == NType::PREFIX) {
		auto &prefix = Prefix::Get(*art, node);
		for (idx_t i = 0; i < prefix.data[Node::PREFIX_SIZE]; i++) {
			current_key.Push(prefix.data[i]);
		}
		nodes.emplace(node, 0);
		return FindMinimum(prefix.ptr);
	}

	// go to the leftmost entry in the current node and recurse
	uint8_t byte = 0;
	auto next = node.GetNextChild(*art, byte);
	D_ASSERT(next);
	current_key.Push(byte);
	nodes.emplace(node, byte);
	FindMinimum(*next);
}

bool Iterator::LowerBound(Node &node, const ARTKey &key, const bool equal, idx_t depth) {

	if (!node.IsSet()) {
		return false;
	}

	if (node.IsSwizzled()) {
		node.Deserialize(*art);
	}

	// we found the lower bound
	if (node.DecodeARTNodeType() == NType::LEAF) {
		if (!equal && current_key == key) {
			return Next();
		}
		last_leaf = Node::GetAllocator(*art, NType::LEAF).Get<Leaf>(node);
		return true;
	}

	if (node.DecodeARTNodeType() != NType::PREFIX) {
		auto next_byte = key[depth];
		auto child = node.GetNextChild(*art, next_byte);
		if (!child) {
			// the key is greater than any key in this subtree
			return Next();
		}

		current_key.Push(next_byte);
		nodes.emplace(node, next_byte);

		if (next_byte > key[depth]) {
			// we only need to find the minimum from here
			// because all keys will be greater than the lower bound
			FindMinimum(*child);
			return true;
		}

		// recurse into the child
		return LowerBound(*child, key, equal, depth + 1);
	}

	// resolve the prefix
	auto &prefix = Prefix::Get(*art, node);
	for (idx_t i = 0; i < prefix.data[Node::PREFIX_SIZE]; i++) {
		current_key.Push(prefix.data[i]);
	}
	nodes.emplace(node, 0);

	for (idx_t i = 0; i < prefix.data[Node::PREFIX_SIZE]; i++) {
		// the key down to this node is less than the lower bound, the next key will be
		// greater than the lower bound
		if (prefix.data[i] < key[depth + i]) {
			return Next();
		}
		// we only need to find the minimum from here
		// because all keys will be greater than the lower bound
		if (prefix.data[i] > key[depth + i]) {
			FindMinimum(prefix.ptr);
			return true;
		}
	}

	// recurse into the child
	depth += prefix.data[Node::PREFIX_SIZE];
	return LowerBound(prefix.ptr, key, equal, depth);
}

bool Iterator::Next() {

	while (!nodes.empty()) {

		auto &top = nodes.top();
		D_ASSERT(top.node.DecodeARTNodeType() != NType::LEAF);

		if (top.node.DecodeARTNodeType() == NType::PREFIX) {
			PopNode();
			continue;
		}

		if (top.byte == NumericLimits<uint8_t>::Maximum()) {
			// no node found: move up the tree, pop key byte of current node
			PopNode();
			continue;
		}

		top.byte++;
		auto next_node = top.node.GetNextChild(*art, top.byte);
		if (!next_node) {
			PopNode();
			continue;
		}

		current_key.Pop(1);
		current_key.Push(top.byte);

		FindMinimum(*next_node);
		return true;
	}
	return false;
}

void Iterator::PopNode() {
	if (nodes.top().node.DecodeARTNodeType() == NType::PREFIX) {
		auto prefix_byte_count = Prefix::Get(*art, nodes.top().node).data[Node::PREFIX_SIZE];
		current_key.Pop(prefix_byte_count);
	} else {
		current_key.Pop(1);
	}
	nodes.pop();
}

} // namespace duckdb
