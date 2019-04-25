#include "execution/index/art/node16.hpp"
#include "execution/index/art/node48.hpp"

using namespace duckdb;

// TODO : In the future this can be performed using SIMD (#include <emmintrin.h>  x86 SSE intrinsics)
// TODO : Use Binary Search instead of scanning all 16 elements
Node *Node16::getChild(const uint8_t k) const {
	for (uint32_t i = 0; i < count; ++i) {
		if (key[i] == k) {
			return child[i];
		}
	}
	return nullptr;
}

void Node16::insert(Node16 *node, Node **nodeRef, uint8_t keyByte, Node *child) {
	if (node->count < 16) {
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
		// Grow to Node48
		Node48 *newNode = new Node48();
		*nodeRef = newNode;
		memcpy(newNode->child, node->child, node->count * sizeof(uintptr_t));
		for (unsigned i = 0; i < node->count; i++)
			newNode->childIndex[node->key[i]] = i;
		copyPrefix(node, newNode);
		newNode->count = node->count;
		delete node;
		return Node48::insert(newNode, nodeRef, keyByte, child);
	}
}
