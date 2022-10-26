#include "duckdb/execution/index/art/iterator.hpp"

#include "duckdb/execution/index/art/art.hpp"

namespace duckdb {
uint8_t &IteratorCurrentKey::operator[](idx_t idx) {
	if (idx >= key.size()) {
		key.push_back(0);
	}
	D_ASSERT(idx < key.size());
	return key[idx];
}

//! Push Byte
void IteratorCurrentKey::Push(uint8_t byte) {
	if (cur_key_pos == key.size()) {
		key.push_back(byte);
	}
	D_ASSERT(cur_key_pos < key.size());
	key[cur_key_pos++] = byte;
}
//! Pops n elements
void IteratorCurrentKey::Pop(idx_t n) {
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

void Iterator::FindMinimum(Node &node) {
	Node *next = nullptr;
	idx_t pos = 0;
	// reconstruct the prefix
	for (idx_t i = 0; i < node.prefix.Size(); i++) {
		cur_key.Push(node.prefix[i]);
	}
	switch (node.type) {
	case NodeType::NLeaf:
		last_leaf = (Leaf *)&node;
		return;
	case NodeType::N4: {
		next = ((Node4 &)node).children[0].Unswizzle(*art);
		cur_key.Push(((Node4 &)node).key[0]);
		break;
	}
	case NodeType::N16: {
		next = ((Node16 &)node).children[0].Unswizzle(*art);
		cur_key.Push(((Node16 &)node).key[0]);
		break;
	}
	case NodeType::N48: {
		auto &n48 = (Node48 &)node;
		while (n48.child_index[pos] == Node::EMPTY_MARKER) {
			pos++;
		}
		cur_key.Push(pos);
		next = n48.children[n48.child_index[pos]].Unswizzle(*art);
		break;
	}
	case NodeType::N256: {
		auto &n256 = (Node256 &)node;
		while (!n256.children[pos]) {
			pos++;
		}
		cur_key.Push(pos);
		next = (Node *)n256.children[pos].Unswizzle(*art);
		break;
	}
	}
	nodes.push(IteratorEntry(&node, pos));
	FindMinimum(*next);
}

void Iterator::PushKey(Node *cur_node, uint16_t pos) {
	switch (cur_node->type) {
	case NodeType::N4:
		cur_key.Push(((Node4 *)cur_node)->key[pos]);
		break;
	case NodeType::N16:
		cur_key.Push(((Node16 *)cur_node)->key[pos]);
		break;
	case NodeType::N48:
	case NodeType::N256:
		cur_key.Push(pos);
		break;
	case NodeType::NLeaf:
		break;
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
			row_t row_id = last_leaf->GetRowId(i);
			result_ids.push_back(row_id);
		}
		has_next = Next();
	} while (has_next);
	return true;
}

void Iterator::PopNode() {
	auto cur_node = nodes.top();
	idx_t elements_to_pop = cur_node.node->prefix.Size() + (nodes.size() != 1);
	cur_key.Pop(elements_to_pop);
	nodes.pop();
}

bool Iterator::Next() {
	if (!nodes.empty()) {
		auto cur_node = nodes.top().node;
		if (cur_node->type == NodeType::NLeaf) {
			// Pop Leaf (We must pop the prefix size + the key to the node (unless we are popping the root)
			PopNode();
		}
	}

	// Look for the next leaf
	while (!nodes.empty()) {
		// cur_node
		auto &top = nodes.top();
		Node *node = top.node;
		if (node->type == NodeType::NLeaf) {
			// found a leaf: move to next node
			last_leaf = (Leaf *)node;
			return true;
		}
		// Find next node
		top.pos = node->GetNextPos(top.pos);
		if (top.pos != DConstants::INVALID_INDEX) {
			// add key-byte of the new node
			PushKey(node, top.pos);
			auto next_node = node->GetChild(*art, top.pos);
			// add prefix of new node
			for (idx_t i = 0; i < next_node->prefix.Size(); i++) {
				cur_key.Push(next_node->prefix[i]);
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

bool Iterator::LowerBound(Node *node, Key &key, bool inclusive) {
	bool equal = true;
	if (!node) {
		return false;
	}
	idx_t depth = 0;
	while (true) {
		nodes.push(IteratorEntry(node, 0));
		auto &top = nodes.top();
		// reconstruct the prefix
		for (idx_t i = 0; i < top.node->prefix.Size(); i++) {
			cur_key.Push(top.node->prefix[i]);
		}
		// greater case: find leftmost leaf node directly
		if (!equal) {
			while (node->type != NodeType::NLeaf) {
				auto min_pos = node->GetMin();
				PushKey(node, min_pos);
				nodes.push(IteratorEntry(node, min_pos));
				node = node->GetChild(*art, min_pos);
				// reconstruct the prefix
				for (idx_t i = 0; i < node->prefix.Size(); i++) {
					cur_key.Push(node->prefix[i]);
				}
				auto &c_top = nodes.top();
				c_top.node = node;
			}
		}
		if (node->type == NodeType::NLeaf) {
			// found a leaf node: check if it is bigger or equal than the current key
			auto leaf = static_cast<Leaf *>(node);
			last_leaf = leaf;
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
		uint32_t mismatch_pos = node->prefix.KeyMismatchPosition(key, depth);
		if (mismatch_pos != node->prefix.Size()) {
			if (node->prefix[mismatch_pos] < key[depth + mismatch_pos]) {
				// Less
				PopNode();
				return Next();
			} else {
				// Greater
				top.pos = DConstants::INVALID_INDEX;
				return Next();
			}
		}

		// prefix matches, search inside the child for the key
		depth += node->prefix.Size();

		top.pos = node->GetChildGreaterEqual(key[depth], equal);
		// The maximum key byte of the current node is less than the key
		// So fall back to the previous node
		if (top.pos == DConstants::INVALID_INDEX) {
			PopNode();
			return Next();
		}
		PushKey(node, top.pos);
		node = node->GetChild(*art, top.pos);
		// This means all children of this node qualify as geq
		depth++;
	}
}

} // namespace duckdb
