#include "duckdb/execution/index/art/node4.hpp"
#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/node48.hpp"

#include <cstring>

namespace duckdb {

Node16::Node16(ART &art, size_t compression_length) : Node(art, NodeType::N16, compression_length) {
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

unique_ptr<Node> *Node16::GetChild(idx_t pos) {
	D_ASSERT(pos < count);
	return &child[pos];
}

idx_t Node16::GetMin() {
	return 0;
}

void Node16::Insert(ART &art, unique_ptr<Node> &node, uint8_t key_byte, unique_ptr<Node> &child) {
	Node16 *n = static_cast<Node16 *>(node.get());

	if (n->count < 16) {
		// Insert element
		idx_t pos = 0;
		while (pos < node->count && n->key[pos] < key_byte) {
			pos++;
		}
		if (n->child[pos] != nullptr) {
			for (idx_t i = n->count; i > pos; i--) {
				n->key[i] = n->key[i - 1];
				n->child[i] = move(n->child[i - 1]);
			}
		}
		n->key[pos] = key_byte;
		n->child[pos] = move(child);
		n->count++;
	} else {
		// Grow to Node48
		auto new_node = make_unique<Node48>(art, n->prefix_length);
		for (idx_t i = 0; i < node->count; i++) {
			new_node->child_index[n->key[i]] = i;
			new_node->child[i] = move(n->child[i]);
		}
		CopyPrefix(art, n, new_node.get());
		new_node->count = node->count;
		node = move(new_node);

		Node48::Insert(art, node, key_byte, child);
	}
}

void Node16::Erase(ART &art, unique_ptr<Node> &node, int pos) {
	Node16 *n = static_cast<Node16 *>(node.get());
	// erase the child and decrease the count
	n->child[pos].reset();
	n->count--;
	// potentially move any children backwards
	for (; pos < n->count; pos++) {
		n->key[pos] = n->key[pos + 1];
		n->child[pos] = move(n->child[pos + 1]);
	}
	if (node->count <= 3) {
		// Shrink node
		auto new_node = make_unique<Node4>(art, n->prefix_length);
		for (unsigned i = 0; i < n->count; i++) {
			new_node->key[new_node->count] = n->key[i];
			new_node->child[new_node->count++] = move(n->child[i]);
		}
		CopyPrefix(art, n, new_node.get());
		node = move(new_node);
	}
}

} // namespace duckdb
