#include "execution/index/art/node16.hpp"
#include "execution/index/art/node48.hpp"
#include "execution/index/art/node256.hpp"

using namespace duckdb;

unique_ptr<Node>* Node48::getChild(const uint8_t k) {
	if (childIndex[k] == emptyMarker) {
		return NULL;
	} else {
		return &child[childIndex[k]];
	}
}

void Node48::insert(unique_ptr<Node>& node, uint8_t keyByte, unique_ptr<Node>& child) {
    Node48 *n = static_cast<Node48 *>(node.get());

    // Insert leaf into inner node
	if (node->count < 48) {
		// Insert element
		unsigned pos = n->count;
		if (n->child[pos])
			for (pos = 0; n->child[pos] != NULL; pos++)
				;
		n->child[pos] = move(child);
		n->childIndex[keyByte] = pos;
		n->count++;
	} else {
		// Grow to Node256
        auto newNode = make_unique<Node256>(node->maxPrefixLength);
        for (unsigned i = 0; i < 256; i++)
			if (n->childIndex[i] != 48)
				newNode->child[i] = move(n->child[n->childIndex[i]]);
		newNode->count = n->count;
		copyPrefix(n, newNode.get());
        node = move(newNode);
        Node256::insert(node, keyByte, child);
	}
}

void Node48::erase(Node48 *node, Node **nodeRef, uint8_t keyByte) {
	// Delete leaf from inner node
//	node->child[node->childIndex[keyByte]] = NULL;
//	node->childIndex[keyByte] = emptyMarker;
//	node->count--;
//
//	if (node->count == 12) {
//		// Shrink to Node16
//		Node16 *newNode = new Node16(node->maxPrefixLength);
//		*nodeRef = newNode;
//		copyPrefix(node, newNode);
//		for (unsigned b = 0; b < 256; b++) {
//			if (node->childIndex[b] != emptyMarker) {
//				newNode->key[newNode->count] = b;
//				newNode->child[newNode->count] = node->child[node->childIndex[b]];
//				newNode->count++;
//			}
//		}
//		delete node;
//	}
}
