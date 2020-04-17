#include "duckdb/execution/index/art/node48.hpp"
#include "duckdb/execution/index/art/node256.hpp"

using namespace duckdb;

Node256::Node256(ART &art, size_t compressionLength) : Node(art, NodeType::N256, compressionLength) {
}

idx_t Node256::GetChildPos(uint8_t k) {
	if (child[k]) {
		return k;
	} else {
		return INVALID_INDEX;
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
	return INVALID_INDEX;
}

idx_t Node256::GetMin() {
	for (idx_t i = 0; i < 256; i++) {
		if (child[i]) {
			return i;
		}
	}
	return INVALID_INDEX;
}

idx_t Node256::GetNextPos(idx_t pos) {
	for (pos == INVALID_INDEX ? pos = 0 : pos++; pos < 256; pos++) {
		if (child[pos]) {
			return pos;
		}
	}
	return Node::GetNextPos(pos);
}

unique_ptr<Node> *Node256::GetChild(idx_t pos) {
	assert(child[pos]);
	return &child[pos];
}

void Node256::insert(ART &art, unique_ptr<Node> &node, uint8_t keyByte, unique_ptr<Node> &child) {
	Node256 *n = static_cast<Node256 *>(node.get());

	n->count++;
	n->child[keyByte] = move(child);
}

void Node256::erase(ART &art, unique_ptr<Node> &node, int pos) {
	Node256 *n = static_cast<Node256 *>(node.get());

	n->child[pos].reset();
	n->count--;
	if (node->count <= 36) {
		auto newNode = make_unique<Node48>(art, n->prefix_length);
		CopyPrefix(art, n, newNode.get());
		for (idx_t i = 0; i < 256; i++) {
			if (n->child[i]) {
				newNode->childIndex[i] = newNode->count;
				newNode->child[newNode->count] = move(n->child[i]);
				newNode->count++;
			}
		}
		node = move(newNode);
	}
}
