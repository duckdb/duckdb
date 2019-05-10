#include "execution/index/art/node4.hpp"
#include "execution/index/art/node16.hpp"
#include "execution/index/art/node48.hpp"

using namespace duckdb;

// TODO : In the future this can be performed using SIMD (#include <emmintrin.h>  x86 SSE intrinsics)
unique_ptr<Node>* Node16::getChild(const uint8_t k) {
	for (uint32_t i = 0; i < count; ++i) {
		if (key[i] == k) {
			return &child[i];
		}
	}
	return nullptr;
}

void Node16::insert(unique_ptr<Node>& node, uint8_t keyByte, unique_ptr<Node>& child){
    Node16 *n = static_cast<Node16 *>(node.get());

if (n->count < 16) {
		// Insert element
		unsigned pos;
		for (pos = 0; (pos < n->count) && (n->key[pos] < keyByte); pos++)
			;
        //FIXME: Don'' use memmove anymore
		memmove(n->key + pos + 1, n->key + pos, n->count - pos);
		memmove(n->child + pos + 1, n->child + pos, (n->count - pos) * sizeof(uintptr_t));
        n->key[pos] = keyByte;
        n->child[pos] = move(child);
        n->count++;
	} else {
		// Grow to Node48
        auto newNode = make_unique<Node48>(node->maxPrefixLength);
        memcpy(newNode->child, n->child, node->count * sizeof(uintptr_t));
		for (unsigned i = 0; i < node->count; i++)
			newNode->childIndex[n->key[i]] = i;
		copyPrefix(n, newNode.get());
		newNode->count = node->count;
		node = move(newNode);

    Node48::insert(node, keyByte, child);
	}
}

void Node16::erase(Node16 *node, Node **nodeRef, Node **leafPlace) {
	// Delete leaf from inner node
//	unsigned pos = leafPlace - node->child;
//	memmove(node->key + pos, node->key + pos + 1, node->count - pos - 1);
//	memmove(node->child + pos, node->child + pos + 1, (node->count - pos - 1) * sizeof(uintptr_t));
//	node->count--;
//
//	if (node->count == 3) {
//		// Shrink to Node4
//		Node4 *newNode = new Node4(node->maxPrefixLength);
//		newNode->count = node->count;
//		copyPrefix(node, newNode);
//		for (unsigned i = 0; i < 4; i++)
//			newNode->key[i] = node->key[i];
//		memcpy(newNode->child, node->child, sizeof(uintptr_t) * 4);
//		*nodeRef = newNode;
//		delete node;
//	}
}
