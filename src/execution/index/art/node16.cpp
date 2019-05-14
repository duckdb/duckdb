#include "execution/index/art/node4.hpp"
#include "execution/index/art/node16.hpp"
#include "execution/index/art/node48.hpp"

using namespace duckdb;

// TODO : In the future this can be performed using SIMD (#include <emmintrin.h>  x86 SSE intrinsics)
unique_ptr<Node> *Node16::getChild(const uint8_t k) {
	for (uint32_t i = 0; i < count; ++i) {
		if (key[i] == k) {
			return &child[i];
		}
	}
	return nullptr;
}

int Node16::getPos(const uint8_t k) {
	int pos;
	for (pos = 0; pos < count; ++pos) {
		if (key[pos] == k) {
			return pos;
		}
	}
	return -1;
}

unique_ptr<Node> *Node16::getMin() {
	return &child[0];
}

void Node16::insert(unique_ptr<Node> &node, uint8_t keyByte, unique_ptr<Node> &child) {
	Node16 *n = static_cast<Node16 *>(node.get());

	if (n->count < 16) {
		// Insert element
		unsigned pos;
		for (pos = 0; (pos < node->count) && (n->key[pos] < keyByte); pos++)
			;
		if (n->child[pos] != nullptr) {
			for (unsigned i = n->count; i > pos; i--) {
				n->key[i] = n->key[i - 1];
				n->child[i] = move(n->child[i - 1]);
			}
		}
		n->key[pos] = keyByte;
		n->child[pos] = move(child);
		n->count++;
	} else {
		// Grow to Node48
		auto newNode = make_unique<Node48>(node->maxPrefixLength);
		for (unsigned i = 0; i < node->count; i++) {
			newNode->childIndex[n->key[i]] = i;
			newNode->child[i] = move(n->child[i]);
		}
		copyPrefix(n, newNode.get());
		newNode->count = node->count;
		node = move(newNode);

		Node48::insert(node, keyByte, child);
	}
}

void Node16::erase(unique_ptr<Node> &node, int pos) {
	Node16 *n = static_cast<Node16 *>(node.get());
	if (node->count > 3) {
		if (n->count == 16) {
			n->child[15].release();
			n->count--;
		} else {
			for (; pos < n->count; pos++) {
				n->key[pos] = n->key[pos + 1];
				n->child[pos] = move(n->child[pos + 1]);
			}
			n->count--;
		}
	}
	// Shrink node
	else {
		auto newNode = make_unique<Node4>(node->maxPrefixLength);
		for (unsigned i = 0; i < n->count; i++) {
			newNode->key[newNode->count] = n->key[i];
			newNode->child[newNode->count++] = move(n->child[i]);
		}
		copyPrefix(n, newNode.get());
		node = move(newNode);
	}
}
