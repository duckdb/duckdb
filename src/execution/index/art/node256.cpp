#include "execution/index/art/node48.hpp"
#include "execution/index/art/node256.hpp"

using namespace duckdb;

unique_ptr<Node>* Node256::getChild(const uint8_t k) {
	if (child[k]) {
		return &child[k];
	} else {
		return NULL;
	}
}

void Node256::insert(unique_ptr<Node>& node, uint8_t keyByte, unique_ptr<Node>& child) {
    Node256 *n = static_cast<Node256 *>(node.get());

    n->count++;
    n->child[keyByte] = move(child);
}

void Node256::erase(Node256 *node, Node **nodeRef, uint8_t keyByte) {
	// Delete leaf from inner node
//	node->child[keyByte] = NULL;
//	node->count--;
//
//	if (node->count == 37) {
//		// Shrink to Node48
//		Node48 *newNode = new Node48(node->maxPrefixLength);
//		*nodeRef = newNode;
//		copyPrefix(node, newNode);
//		for (unsigned b = 0; b < 256; b++) {
//			if (node->child[b]) {
//				newNode->childIndex[b] = newNode->count;
//				newNode->child[newNode->count] = node->child[b];
//				newNode->count++;
//			}
//		}
//		delete node;
//	}
}
