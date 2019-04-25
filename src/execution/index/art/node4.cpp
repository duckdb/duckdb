#include "execution/index/art/node4.hpp"
#include "execution/index/art/node16.hpp"

using namespace duckdb;

Node *Node4::getChild(const uint8_t k) const {
	for (uint32_t i = 0; i < count; ++i) {
		if (key[i] == k) {
			return child[i];
		}
	}
	return nullptr;
}

void Node4::insert(Node4 *node, Node **nodeRef, uint8_t keyByte, Node *child) {
	// Insert leaf into inner node
	if (node->count < 4) {
		// Insert element
		unsigned pos;
		for (pos = 0; (pos < node->count) && (node->key[pos] < keyByte); pos++)
			;
		memmove(node->key + pos + 1, node->key + pos, node->count - pos);
		memmove(node->child + pos + 1, node->child + pos, (node->count - pos) * sizeof(uintptr_t));
		node->key[pos] = keyByte;
		node->child[pos] = child;
		node->count++;
	} else {
		// Grow to Node16
		Node16 *newNode = new Node16();
		*nodeRef = newNode;
		newNode->count = 4;
		copyPrefix(node, newNode);
		for (unsigned i = 0; i < 4; i++)
			newNode->key[i] = node->key[i];
		memcpy(newNode->child, node->child, node->count * sizeof(uintptr_t));
		delete node;
		return Node16::insert(newNode, nodeRef, keyByte, child);
	}
}
