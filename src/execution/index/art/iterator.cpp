#include "duckdb/execution/index/art/iterator.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_node.hpp"
#include "duckdb/execution/index/art/prefix.hpp"

namespace duckdb {

uint8_t &IteratorCurrentKey::operator[](idx_t idx) {
	if (idx >= key.size()) {
		key.push_back(0);
	}
	D_ASSERT(idx < key.size());
	return key[idx];
}

void IteratorCurrentKey::Push(const uint8_t &byte) {
	if (cur_key_pos == key.size()) {
		key.push_back(byte);
	}
	D_ASSERT(cur_key_pos < key.size());
	key[cur_key_pos++] = byte;
}

void IteratorCurrentKey::Pop(const idx_t &n) {
	cur_key_pos -= n;
	D_ASSERT(cur_key_pos <= key.size());
}

bool IteratorCurrentKey::operator>(const Key &k) const {
	for (idx_t i = 0; i < MinValue<idx_t>(cur_key_pos, k.len); i++) {
		if (key[i] > k.data[i]) {
			return true;
		} else if (key[i] < k.data[i]) {
			return false;
		}
	}
	return cur_key_pos > k.len;
}

bool IteratorCurrentKey::operator>=(const Key &k) const {
	for (idx_t i = 0; i < MinValue<idx_t>(cur_key_pos, k.len); i++) {
		if (key[i] > k.data[i]) {
			return true;
		} else if (key[i] < k.data[i]) {
			return false;
		}
	}
	return cur_key_pos >= k.len;
}

bool IteratorCurrentKey::operator==(const Key &k) const {
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

void Iterator::FindMinimum(ARTNode &node) {

	// reconstruct the prefix
	auto node_prefix = node.GetPrefix(*art);
	for (idx_t i = 0; i < node_prefix->count; i++) {
		cur_key.Push(node_prefix->GetByte(*art, i));
	}

	// found the minimum
	if (node.DecodeARTNodeType() == ARTNodeType::NLeaf) {
		last_leaf = node.Get<Leaf>(art->leaf_nodes);
		return;
	}

	// go to the leftmost entry in the current node
	auto position = node.GetMinPos(*art);
	D_ASSERT(position != DConstants::INVALID_INDEX);
	auto next = node.GetChild(*art, position);
	cur_key.Push(node.GetKeyByte(*art, position));

	// recurse
	nodes.push(IteratorEntry(node, position));
	FindMinimum(next);
}

void Iterator::PushKey(ARTNode &cur_node, uint16_t pos) {
	if (cur_node.DecodeARTNodeType() != ARTNodeType::NLeaf) {
		cur_key.Push(cur_node.GetKeyByte(*art, pos));
	}
}

bool Iterator::Scan(Key &bound, idx_t max_count, vector<row_t> &result_ids, bool is_inclusive) {
	bool has_next;
	do {
		if (!bound.Empty()) {
			if (is_inclusive) {
				if (cur_key > bound) {
					break;
				}
			} else {
				if (cur_key >= bound) {
					break;
				}
			}
		}
		if (result_ids.size() + last_leaf->count > max_count) {
			// adding these elements would exceed the max count
			return false;
		}
		for (idx_t i = 0; i < last_leaf->count; i++) {
			row_t row_id = last_leaf->GetRowId(*art, i);
			result_ids.push_back(row_id);
		}
		has_next = Next();
	} while (has_next);
	return true;
}

void Iterator::PopNode() {
	auto cur_node = nodes.top();
	idx_t elements_to_pop = cur_node.node.GetPrefix(*art)->count + (nodes.size() != 1);
	cur_key.Pop(elements_to_pop);
	nodes.pop();
}

bool Iterator::Next() {
	if (!nodes.empty()) {
		auto cur_node = nodes.top().node;
		if (cur_node.DecodeARTNodeType() == ARTNodeType::NLeaf) {
			// Pop Leaf (We must pop the prefix size + the key to the node (unless we are popping the root)
			PopNode();
		}
	}

	// look for the next leaf
	while (!nodes.empty()) {
		// cur_node
		auto &top = nodes.top();
		ARTNode node = top.node;
		if (node.DecodeARTNodeType() == ARTNodeType::NLeaf) {
			// found a leaf: move to next node
			last_leaf = node.Get<Leaf>(art->leaf_nodes);
			return true;
		}
		// find next node
		top.position = node.GetNextPos(*art, top.position);
		if (top.position != DConstants::INVALID_INDEX) {
			// add key-byte of the new node
			PushKey(node, top.position);
			auto next_node = node.GetChild(*art, top.position);
			// add prefix of new node
			auto next_node_prefix = node.GetPrefix(*art);
			for (idx_t i = 0; i < next_node_prefix->count; i++) {
				cur_key.Push(next_node_prefix->GetByte(*art, i));
			}
			// next node found: push it
			nodes.push(IteratorEntry(next_node, DConstants::INVALID_INDEX));
		} else {
			// no node found: move up the tree and Pop prefix and key of current node
			PopNode();
		}
	}
	return false;
}

bool Iterator::LowerBound(ARTNode &node, Key &key, bool inclusive) {
	bool equal = true;
	if (!node) {
		return false;
	}
	idx_t depth = 0;
	while (true) {
		nodes.push(IteratorEntry(node, 0));
		auto &top = nodes.top();

		// reconstruct the prefix
		auto node_prefix = top.node.GetPrefix(*art);
		for (idx_t i = 0; i < node_prefix->count; i++) {
			cur_key.Push(node_prefix->GetByte(*art, i));
		}

		// greater case: find leftmost leaf node directly
		if (!equal) {
			while (node.DecodeARTNodeType() != ARTNodeType::NLeaf) {
				auto min_pos = node.GetMinPos(*art);
				PushKey(node, min_pos);
				nodes.push(IteratorEntry(node, min_pos));
				node = node.GetChild(*art, min_pos);

				// reconstruct the prefix
				node_prefix = node.GetPrefix(*art);
				for (idx_t i = 0; i < node_prefix->count; i++) {
					cur_key.Push(node_prefix->GetByte(*art, i));
				}

				auto &c_top = nodes.top();
				c_top.node = node;
			}
		}

		if (node.DecodeARTNodeType() == ARTNodeType::NLeaf) {
			// found a leaf node: check if it is bigger or equal than the current key
			last_leaf = node.Get<Leaf>(art->leaf_nodes);

			// if the search is not inclusive the leaf node could still be equal to the current value
			// check if leaf is equal to the current key
			if (cur_key == key) {
				// if it's not inclusive check if there is a next leaf
				if (!inclusive && !Next()) {
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
		uint32_t mismatch_pos = node_prefix->KeyMismatchPosition(*art, key, depth);
		if (mismatch_pos != node_prefix->count) {
			if (node_prefix->GetByte(*art, mismatch_pos) < key[depth + mismatch_pos]) {
				// Less
				PopNode();
				return Next();
			}
			// Greater
			top.position = DConstants::INVALID_INDEX;
			return Next();
		}

		// prefix matches, search inside the child for the key
		depth += node_prefix->count;

		top.position = node.GetChildPosGreaterEqual(*art, key[depth], equal);
		// The maximum key byte of the current node is less than the key
		// So fall back to the previous node
		if (top.position == DConstants::INVALID_INDEX) {
			PopNode();
			return Next();
		}
		PushKey(node, top.position);
		node = node.GetChild(*art, top.position);
		// This means all children of this node qualify as geq
		depth++;
	}
}

} // namespace duckdb
