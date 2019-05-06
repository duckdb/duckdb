#include "execution/index/art/node16.hpp"
#include "execution/index/art/node48.hpp"
#include "execution/index/art/node256.hpp"

using namespace duckdb;

Node **Node48::getChild(const uint8_t k) {
	if (childIndex[k] == emptyMarker) {
		return NULL;
	} else {
		return &child[childIndex[k]];
	}
}

void Node48::insert(Node48 *node, Node **nodeRef, uint8_t keyByte, Node *child) {
	// Insert leaf into inner node
	if (node->count < 48) {
		// Insert element
		unsigned pos = node->count;
		if (node->child[pos])
			for (pos = 0; node->child[pos] != NULL; pos++);
		node->child[pos] = child;
		node->childIndex[keyByte] = pos;
		node->count++;
	} else {
		// Grow to Node256
		Node256 *newNode = new Node256();
		for (unsigned i = 0; i < 256; i++)
			if (node->childIndex[i] != 48)
				newNode->child[i] = node->child[node->childIndex[i]];
		newNode->count = node->count;
		copyPrefix(node, newNode);
		*nodeRef = newNode;
		delete node;
		return Node256::insert(newNode, nodeRef, keyByte, child);
	}
}

void Node48::erase(Node48* node,Node** nodeRef,uint8_t keyByte) {
		// Delete leaf from inner node
		node->child[node->childIndex[keyByte]]=NULL;
		node->childIndex[keyByte]=emptyMarker;
		node->count--;

		if (node->count==12) {
			// Shrink to Node16
			Node16 *newNode=new Node16();
			*nodeRef=newNode;
			copyPrefix(node,newNode);
			for (unsigned b=0;b<256;b++) {
				if (node->childIndex[b]!=emptyMarker) {
					newNode->key[newNode->count]=b;
					newNode->child[newNode->count]=node->child[node->childIndex[b]];
					newNode->count++;
				}
			}
			delete node;
		}
}

