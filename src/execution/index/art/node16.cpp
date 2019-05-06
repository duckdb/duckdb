#include "execution/index/art/node4.hpp"
#include "execution/index/art/node16.hpp"
#include "execution/index/art/node48.hpp"

using namespace duckdb;


// TODO : In the future this can be performed using SIMD (#include <emmintrin.h>  x86 SSE intrinsics)
Node **Node16::getChild(const uint8_t k) {
	for (uint32_t i = 0; i < count; ++i) {
		if (key[i] == k) {
			return &child[i];
		}
	}
	return NULL;
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

void Node16::erase(Node16* node,Node** nodeRef,Node** leafPlace) {
	// Delete leaf from inner node
	unsigned pos=leafPlace-node->child;
	memmove(node->key+pos,node->key+pos+1,node->count-pos-1);
	memmove(node->child+pos,node->child+pos+1,(node->count-pos-1)*sizeof(uintptr_t));
	node->count--;

	if (node->count==3) {
		// Shrink to Node4
		Node4* newNode=new Node4();
		newNode->count=node->count;
		copyPrefix(node,newNode);
		for (unsigned i=0;i<4;i++)
			newNode->key[i]=node->key[i];
		memcpy(newNode->child,node->child,sizeof(uintptr_t)*4);
		*nodeRef=newNode;
		delete node;
	}
}