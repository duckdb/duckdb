#include "duckdb/execution/index/art/node4.hpp"
#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/node48.hpp"

#include <cstring>

namespace duckdb {

Node16::Node16(size_t compression_length) : Node(NodeType::N16, compression_length) {
	memset(key, 16, sizeof(key));
}

idx_t Node16::GetChildPos(uint8_t k) {
	for (idx_t pos = 0; pos < count; pos++) {
		if (key[pos] == k) {
			return pos;
		}
	}
	return Node::GetChildPos(k);
}

idx_t Node16::GetChildGreaterEqual(uint8_t k, bool &equal) {
	for (idx_t pos = 0; pos < count; pos++) {
		if (key[pos] >= k) {
			if (key[pos] == k) {
				equal = true;
			} else {
				equal = false;
			}

			return pos;
		}
	}
	return Node::GetChildGreaterEqual(k, equal);
}

idx_t Node16::GetNextPos(idx_t pos) {
	if (pos == DConstants::INVALID_INDEX) {
		return 0;
	}
	pos++;
	return pos < count ? pos : DConstants::INVALID_INDEX;
}

Node *Node16::GetChild(ART &art, idx_t pos) {
	D_ASSERT(pos < count);
	return children[pos].Unswizzle(art);
}

idx_t Node16::GetMin() {
	return 0;
}

void Node16::ReplaceChildPointer(idx_t pos, Node *node) {
	children[pos] = node;
}

void Node16::Insert(Node *&node, uint8_t key_byte, Node *child) {
	Node16 *n = (Node16 *)node;

	if (n->count < 16) {
		// Insert element
		idx_t pos = 0;
		while (pos < node->count && n->key[pos] < key_byte) {
			pos++;
		}
		if (n->children[pos] != 0) {
			for (idx_t i = n->count; i > pos; i--) {
				n->key[i] = n->key[i - 1];
				n->children[i] = n->children[i - 1];
			}
		}
		n->key[pos] = key_byte;
		n->children[pos] = child;
		n->count++;
	} else {
		// Grow to Node48
		auto new_node = new Node48(n->prefix_length);
		for (idx_t i = 0; i < node->count; i++) {
			new_node->child_index[n->key[i]] = i;
			new_node->children[i] = n->children[i];
			n->children[i] = nullptr;
		}
		CopyPrefix(n, new_node);
		new_node->count = node->count;
		delete node;
		node = new_node;

		Node48::Insert(node, key_byte, child);
	}
}

void Node16::Erase(Node *&node, int pos, ART &art) {
	auto n = (Node16 *)node;
	// erase the child and decrease the count
	n->children[pos].Reset();
	n->count--;
	// potentially move any children backwards
	for (; pos < n->count; pos++) {
		n->key[pos] = n->key[pos + 1];
		n->children[pos] = n->children[pos + 1];
	}
	// set any remaining nodes as nullptr
	for (; pos < 16; pos++) {
		if (!n->children[pos].pointer) {
			break;
		}
		n->children[pos] = nullptr;
	}

	if (node->count <= 3) {
		// Shrink node
		auto new_node = new Node4(n->prefix_length);
		for (unsigned i = 0; i < n->count; i++) {
			new_node->key[new_node->count] = n->key[i];
			new_node->children[new_node->count++] = n->children[i];
			n->children[i] = nullptr;
		}
		CopyPrefix(n, new_node);
		delete node;
		node = new_node;
	}
}

} // namespace duckdb
