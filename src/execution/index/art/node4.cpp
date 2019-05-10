#include "execution/index/art/node4.hpp"
#include "execution/index/art/node16.hpp"

using namespace duckdb;

Node **Node4::getChild(const uint8_t k) {
	for (uint32_t i = 0; i < count; ++i) {
		if (key[i] == k) {
			return &child[i];
		}
	}
	return NULL;
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
		Node16 *newNode = new Node16(node->maxPrefixLength);
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

void Node4::erase(Node4 *node, Node **nodeRef, Node **leafPlace) {
	// Delete leaf from inner node
	unsigned pos = leafPlace - node->child;
	memmove(node->key + pos, node->key + pos + 1, node->count - pos - 1);
	memmove(node->child + pos, node->child + pos + 1, (node->count - pos - 1) * sizeof(uintptr_t));
	node->count--;

	if (node->count == 1) {
		// Get rid of one-way node
		Node *child = node->child[0];
		if (child->type == NodeType::NLeaf) {
			// Concantenate prefixes
			unsigned l1 = node->prefixLength;
			if (l1 < node->maxPrefixLength) {
				node->prefix[l1] = node->key[0];
				l1++;
			}
			if (l1 < node->maxPrefixLength) {
				unsigned l2 = min(child->prefixLength, node->maxPrefixLength - l1);
				memcpy(node->prefix.get() + l1, child->prefix.get(), l2);
				l1 += l2;
			}
			// Store concantenated prefix
			memcpy(child->prefix.get(), node->prefix.get(), min(l1, node->maxPrefixLength));
			child->prefixLength += node->prefixLength + 1;
		}
		*nodeRef = child;
		delete node;
	}
}
