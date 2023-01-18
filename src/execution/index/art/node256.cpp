#include "duckdb/execution/index/art/node256.hpp"

#include "duckdb/execution/index/art/node48.hpp"

namespace duckdb {

Node256::Node256() : Node(NodeType::N256) {
}

idx_t Node256::GetChildPos(uint8_t k) {
	if (children[k]) {
		return k;
	} else {
		return DConstants::INVALID_INDEX;
	}
}

idx_t Node256::GetChildGreaterEqual(uint8_t k, bool &equal) {
	for (idx_t pos = k; pos < 256; pos++) {
		if (children[pos]) {
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
		if (children[i]) {
			return i;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t Node256::GetNextPos(idx_t pos) {
	for (pos == DConstants::INVALID_INDEX ? pos = 0 : pos++; pos < 256; pos++) {
		if (children[pos]) {
			return pos;
		}
	}
	return Node::GetNextPos(pos);
}

idx_t Node256::GetNextPosAndByte(idx_t pos, uint8_t &byte) {
	for (pos == DConstants::INVALID_INDEX ? pos = 0 : pos++; pos < 256; pos++) {
		if (children[pos]) {
			byte = uint8_t(pos);
			return pos;
		}
	}
	return Node::GetNextPos(pos);
}

Node *Node256::GetChild(ART &art, idx_t pos) {
	return children[pos].Unswizzle(art);
}

void Node256::ReplaceChildPointer(idx_t pos, Node *node) {
	children[pos] = node;
}

void Node256::InsertChild(Node *&node, uint8_t key_byte, Node *new_child) {
	auto n = (Node256 *)(node);

	n->count++;
	n->children[key_byte] = new_child;
}

void Node256::EraseChild(Node *&node, int pos, ART &art) {
	auto n = (Node256 *)(node);
	n->children[pos].Reset();
	n->count--;
	if (node->count <= 36) {
		auto new_node = Node48::New();
		new_node->prefix = std::move(n->prefix);
		for (idx_t i = 0; i < 256; i++) {
			if (n->children[i]) {
				new_node->child_index[i] = new_node->count;
				new_node->children[new_node->count] = n->children[i];
				n->children[i] = nullptr;
				new_node->count++;
			}
		}
		Node::Delete(node);
		node = new_node;
	}
}

idx_t Node256::GetSize() {
	return 256;
}

} // namespace duckdb
