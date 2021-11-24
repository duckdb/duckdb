#include "duckdb/execution/index/art/node48.hpp"
#include "duckdb/execution/index/art/node256.hpp"

namespace duckdb {

Node256::Node256(ART &art, size_t compression_length) : Node(art, NodeType::N256, compression_length) {
}

idx_t Node256::GetChildPos(uint8_t k) {
	if (child[k]) {
		return k;
	} else {
		return DConstants::INVALID_INDEX;
	}
}

idx_t Node256::GetChildGreaterEqual(uint8_t k, bool &equal) {
	for (idx_t pos = k; pos < 256; pos++) {
		if (child[pos]) {
			if (pos == k) {
				equal = true;
			} else {
				equal = false;
			}
			return pos;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t Node256::GetMin() {
	for (idx_t i = 0; i < 256; i++) {
		if (child[i]) {
			return i;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t Node256::GetNextPos(idx_t pos) {
	for (pos == DConstants::INVALID_INDEX ? pos = 0 : pos++; pos < 256; pos++) {
		if (child[pos]) {
			return pos;
		}
	}
	return Node::GetNextPos(pos);
}

unique_ptr<Node> *Node256::GetChild(idx_t pos) {
	D_ASSERT(child[pos]);
	return &child[pos];
}

void Node256::Insert(ART &art, unique_ptr<Node> &node, uint8_t key_byte, unique_ptr<Node> &child) {
	Node256 *n = static_cast<Node256 *>(node.get());

	n->count++;
	n->child[key_byte] = move(child);
}

void Node256::Erase(ART &art, unique_ptr<Node> &node, int pos) {
	Node256 *n = static_cast<Node256 *>(node.get());

	n->child[pos].reset();
	n->count--;
	if (node->count <= 36) {
		auto new_node = make_unique<Node48>(art, n->prefix_length);
		CopyPrefix(art, n, new_node.get());
		for (idx_t i = 0; i < 256; i++) {
			if (n->child[i]) {
				new_node->child_index[i] = new_node->count;
				new_node->child[new_node->count] = move(n->child[i]);
				new_node->count++;
			}
		}
		node = move(new_node);
	}
}

} // namespace duckdb
