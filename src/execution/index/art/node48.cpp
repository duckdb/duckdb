#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/node48.hpp"
#include "duckdb/execution/index/art/node256.hpp"

namespace duckdb {

Node48::Node48(size_t compression_length) : Node(NodeType::N48, compression_length) {
	for (idx_t i = 0; i < 256; i++) {
		child_index[i] = Node::EMPTY_MARKER;
	}
}

idx_t Node48::GetChildPos(uint8_t k) {
	if (child_index[k] == Node::EMPTY_MARKER) {
		return DConstants::INVALID_INDEX;
	} else {
		return k;
	}
}

idx_t Node48::GetChildGreaterEqual(uint8_t k, bool &equal) {
	for (idx_t pos = k; pos < 256; pos++) {
		if (child_index[pos] != Node::EMPTY_MARKER) {
			if (pos == k) {
				equal = true;
			} else {
				equal = false;
			}
			return pos;
		}
	}
	return Node::GetChildGreaterEqual(k, equal);
}

idx_t Node48::GetNextPos(idx_t pos) {
	for (pos == DConstants::INVALID_INDEX ? pos = 0 : pos++; pos < 256; pos++) {
		if (child_index[pos] != Node::EMPTY_MARKER) {
			return pos;
		}
	}
	return Node::GetNextPos(pos);
}

Node *Node48::GetChild(ART &art, idx_t pos) {
	D_ASSERT(child_index[pos] != Node::EMPTY_MARKER);
	return children[child_index[pos]].Unswizzle(art);
}

idx_t Node48::GetMin() {
	for (idx_t i = 0; i < 256; i++) {
		if (child_index[i] != Node::EMPTY_MARKER) {
			return i;
		}
	}
	return DConstants::INVALID_INDEX;
}

void Node48::Insert(Node *&node, uint8_t key_byte, Node *child) {
	auto n = (Node48 *)node;

	// Insert leaf into inner node
	if (node->count < 48) {
		// Insert element
		idx_t pos = n->count;
		if (n->children[pos].pointer) {
			// find an empty position in the node list if the current position is occupied
			pos = 0;
			while (n->children[pos].pointer) {
				pos++;
			}
		}
		n->children[pos] = child;
		n->child_index[key_byte] = pos;
		n->count++;
	} else {
		// Grow to Node256
		auto new_node = new Node256(n->prefix_length);
		for (idx_t i = 0; i < 256; i++) {
			if (n->child_index[i] != Node::EMPTY_MARKER) {
				new_node->children[i] = n->children[n->child_index[i]];
				n->children[n->child_index[i]] = nullptr;
			}
		}
		new_node->count = n->count;
		CopyPrefix(n, new_node);
		delete node;
		node = new_node;
		Node256::Insert(node, key_byte, child);
	}
}

void Node48::ReplaceChildPointer(idx_t pos, Node *node) {
	children[child_index[pos]] = node;
}

void Node48::Erase(Node *&node, int pos, ART &art) {
	auto n = (Node48 *)(node);
	n->children[n->child_index[pos]].Reset();
	n->child_index[pos] = Node::EMPTY_MARKER;
	n->count--;
	if (node->count <= 12) {
		auto new_node = new Node16(n->prefix_length);
		CopyPrefix(n, new_node);
		for (idx_t i = 0; i < 256; i++) {
			if (n->child_index[i] != Node::EMPTY_MARKER) {
				new_node->key[new_node->count] = i;
				new_node->children[new_node->count++] = n->children[n->child_index[i]];
				n->children[n->child_index[i]] = nullptr;
			}
		}
		delete node;
		node = new_node;
	}
}

} // namespace duckdb
