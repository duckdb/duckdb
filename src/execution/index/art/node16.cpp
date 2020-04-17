#include "duckdb/execution/index/art/node4.hpp"
#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/node48.hpp"

#include <cstring>

using namespace duckdb;

Node16::Node16(ART &art, size_t compressionLength) : Node(art, NodeType::N16, compressionLength) {
	memset(key, 16, sizeof(key));
}

// TODO : In the future this can be performed using SIMD (#include <emmintrin.h>  x86 SSE intrinsics)
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
	if (pos == INVALID_INDEX) {
		return 0;
	}
	pos++;
	return pos < count ? pos : INVALID_INDEX;
}

unique_ptr<Node> *Node16::GetChild(idx_t pos) {
	assert(pos < count);
	return &child[pos];
}

idx_t Node16::GetMin() {
	return 0;
}

void Node16::insert(ART &art, unique_ptr<Node> &node, uint8_t keyByte, unique_ptr<Node> &child) {
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
		auto newNode = make_unique<Node48>(art, n->prefix_length);
		for (unsigned i = 0; i < node->count; i++) {
			newNode->childIndex[n->key[i]] = i;
			newNode->child[i] = move(n->child[i]);
		}
		CopyPrefix(art, n, newNode.get());
		newNode->count = node->count;
		node = move(newNode);

		Node48::insert(art, node, keyByte, child);
	}
}

void Node16::erase(ART &art, unique_ptr<Node> &node, int pos) {
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
		auto newNode = make_unique<Node4>(art, n->prefix_length);
		for (unsigned i = 0; i < n->count; i++) {
			newNode->key[newNode->count] = n->key[i];
			newNode->child[newNode->count++] = move(n->child[i]);
		}
		CopyPrefix(art, n, newNode.get());
		node = move(newNode);
	}
}
