#include "duckdb/execution/index/art/iterator.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/execution/index/art/prefix.hpp"

namespace duckdb {

void IteratorCurrentKey::Push(const uint8_t byte) {
	if (cur_key_pos == key.size()) {
		key.push_back(byte);
	}
	D_ASSERT(cur_key_pos < key.size());
	key[cur_key_pos++] = byte;
}

void IteratorCurrentKey::Pop(const idx_t n) {
	cur_key_pos -= n;
	D_ASSERT(cur_key_pos <= key.size());
}

uint8_t &IteratorCurrentKey::operator[](idx_t idx) {
	if (idx >= key.size()) {
		key.push_back(0);
	}
	D_ASSERT(idx < key.size());
	return key[idx];
}

bool IteratorCurrentKey::operator>(const ARTKey &k) const {
	for (idx_t i = 0; i < MinValue<idx_t>(cur_key_pos, k.len); i++) {
		if (key[i] > k.data[i]) {
			return true;
		} else if (key[i] < k.data[i]) {
			return false;
		}
	}
	return cur_key_pos > k.len;
}

bool IteratorCurrentKey::operator>=(const ARTKey &k) const {
	for (idx_t i = 0; i < MinValue<idx_t>(cur_key_pos, k.len); i++) {
		if (key[i] > k.data[i]) {
			return true;
		} else if (key[i] < k.data[i]) {
			return false;
		}
	}
	return cur_key_pos >= k.len;
}

bool IteratorCurrentKey::operator==(const ARTKey &k) const {
	if (cur_key_pos != k.len) {
		return false;
	}
	for (idx_t i = 0; i < cur_key_pos; i++) {
		if (key[i] != k.data[i]) {
			return false;
		}
	}
	return true;
}

void Iterator::FindMinimum(Node &node) {

	// reconstruct the prefix
	// FIXME: get all bytes at once to increase performance
	auto &node_prefix = node.GetPrefix(*art);
	for (idx_t i = 0; i < node_prefix.count; i++) {
		cur_key.Push(node_prefix.GetByte(*art, i));
	}

	// found the minimum
	if (node.DecodeARTNodeType() == NType::LEAF) {
		last_leaf = Node::GetAllocator(*art, NType::LEAF).Get<Leaf>(node);
		return;
	}

	// go to the leftmost entry in the current node
	uint8_t byte = 0;
	auto next = node.GetNextChild(*art, byte);
	D_ASSERT(next);
	cur_key.Push(byte);

	// recurse
	nodes.emplace(node, byte);
	FindMinimum(*next);
}

void Iterator::PushKey(const Node &node, const uint8_t byte) {
	if (node.DecodeARTNodeType() != NType::LEAF) {
		cur_key.Push(byte);
	}
}

bool Iterator::Scan(const ARTKey &key, const idx_t &max_count, vector<row_t> &result_ids, const bool &is_inclusive) {

	bool has_next;
	do {
		if (!key.Empty()) {
			// no more row IDs within the key bounds
			if (is_inclusive) {
				if (cur_key > key) {
					return true;
				}
			} else {
				if (cur_key >= key) {
					return true;
				}
			}
		}

		// adding more elements would exceed the max count
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

void Iterator::PopNode() {
	auto cur_node = nodes.top();
	idx_t elements_to_pop = cur_node.node.GetPrefix(*art).count + (nodes.size() != 1);
	cur_key.Pop(elements_to_pop);
	nodes.pop();
}

bool Iterator::Next() {
	if (!nodes.empty()) {
		auto cur_node = nodes.top().node;
		if (cur_node.DecodeARTNodeType() == NType::LEAF) {
			// pop leaf
			// we must pop the prefix size + the key to the node, unless we are popping the root
			PopNode();
		}
	}

	// look for the next leaf
	while (!nodes.empty()) {

		// cur_node
		auto &top = nodes.top();
		Node node = top.node;

		// found a leaf: move to next node
		if (node.DecodeARTNodeType() == NType::LEAF) {
			last_leaf = Node::GetAllocator(*art, NType::LEAF).Get<Leaf>(node);
			return true;
		}

		// find next node
		if (top.byte == NumericLimits<uint8_t>::Maximum()) {
			// no node found: move up the tree, pop prefix and key of current node
			PopNode();
			continue;
		}

		top.byte == 0 ? top.byte : top.byte++;
		auto next_node = node.GetNextChild(*art, top.byte);

		if (next_node) {
			// add the next node's key byte
			PushKey(node, top.byte);

			// add prefix of new node
			// FIXME: get all bytes at once to increase performance
			auto &next_node_prefix = next_node->GetPrefix(*art);
			for (idx_t i = 0; i < next_node_prefix.count; i++) {
				cur_key.Push(next_node_prefix.GetByte(*art, i));
			}

			// next node found: push it
			nodes.emplace(*next_node, 0);
		} else {

			// no node found: move up the tree, pop prefix and key of current node
			PopNode();
		}
	}
	return false;
}

bool Iterator::LowerBound(Node node, const ARTKey &key, const bool &is_inclusive) {

	if (!node.IsSet()) {
		return false;
	}

	idx_t depth = 0;
	bool equal = true;
	while (true) {

		nodes.emplace(node, 0);
		auto &top = nodes.top();

		// reconstruct the prefix
		// FIXME: get all bytes at once to increase performance
		reference<Prefix> node_prefix(top.node.GetPrefix(*art));
		for (idx_t i = 0; i < node_prefix.get().count; i++) {
			cur_key.Push(node_prefix.get().GetByte(*art, i));
		}

		// greater case: find leftmost leaf node directly
		if (!equal) {
			while (node.DecodeARTNodeType() != NType::LEAF) {

				uint8_t byte = 0;
				auto next_node = *node.GetNextChild(*art, byte);
				D_ASSERT(next_node.IsSet());

				PushKey(node, byte);
				nodes.emplace(node, byte);
				node = next_node;

				// reconstruct the prefix
				node_prefix = node.GetPrefix(*art);
				for (idx_t i = 0; i < node_prefix.get().count; i++) {
					cur_key.Push(node_prefix.get().GetByte(*art, i));
				}

				auto &c_top = nodes.top();
				c_top.node = node;
			}
		}

		if (node.DecodeARTNodeType() == NType::LEAF) {
			// found a leaf node: check if it is bigger or equal than the current key
			last_leaf = Node::GetAllocator(*art, NType::LEAF).Get<Leaf>(node);

			// if the search is not inclusive the leaf node could still be equal to the current value
			// check if leaf is equal to the current key
			if (cur_key == key) {
				// if it's not inclusive check if there is a next leaf
				if (!is_inclusive && !Next()) {
					return false;
				} else {
					return true;
				}
			}

			if (cur_key > key) {
				return true;
			}
			// Case1: When the ART has only one leaf node, the Next() will return false
			// Case2: This means the previous node prefix(if any) + a_key(one element of of key array of previous node)
			// == key[q..=w].
			// But key[w+1..=z] maybe greater than leaf node prefix.
			// One fact is key[w] is alawys equal to a_key and the next element
			// of key array of previous node is always > a_key So we just call Next() once.

			return Next();
		}

		// equal case:
		node_prefix = node.GetPrefix(*art);
		auto mismatch_pos = node_prefix.get().KeyMismatchPosition(*art, key, depth);
		if (mismatch_pos != node_prefix.get().count) {
			if (node_prefix.get().GetByte(*art, mismatch_pos) < key[depth + mismatch_pos]) {
				// less
				PopNode();
				return Next();
			}
			// greater
			top.byte = 0;
			return Next();
		}

		// prefix matches, search inside the child for the key
		depth += node_prefix.get().count;
		top.byte = key[depth];
		auto child = node.GetNextChild(*art, top.byte);
		equal = key[depth] == top.byte;

		// the maximum key byte of the current node is less than the key
		// fall back to the previous node
		if (!child) {
			PopNode();
			return Next();
		}

		PushKey(node, top.byte);
		node = *child;

		// all children of this node qualify as greater or equal
		depth++;
	}
}

} // namespace duckdb
