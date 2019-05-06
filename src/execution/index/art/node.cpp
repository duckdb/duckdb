#include "execution/index/art/node.hpp"
#include "execution/index/art/node4.hpp"
#include "execution/index/art/node16.hpp"
#include "execution/index/art/node48.hpp"
#include "execution/index/art/node256.hpp"

using namespace duckdb;



unsigned Node::min(unsigned a, unsigned b) {
	return (a < b) ? a : b;
}


void Node::copyPrefix(Node *src, Node *dst) {
	dst->prefixLength = src->prefixLength;
	memcpy(dst->prefix, src->prefix, min(src->prefixLength, src->maxPrefixLength));
}

Node *Node::minimum(Node *node) {
	if (!node)
		return NULL;

	if (node->type == NodeType::NLeaf)
		return node;

	switch (node->type) {
	case NodeType::N4: {
		Node4 *n = static_cast<Node4 *>(node);
		return minimum(n->child[0]);
	}
	case NodeType::N16: {
		Node16 *n = static_cast<Node16 *>(node);
		return minimum(n->child[0]);
	}
	case NodeType::N48: {
		Node48 *n = static_cast<Node48 *>(node);
		unsigned pos = 0;
		while (n->childIndex[pos] == emptyMarker)
			pos++;
		return minimum(n->child[n->childIndex[pos]]);
	}
	case NodeType::N256: {
		Node256 *n = static_cast<Node256 *>(node);
		unsigned pos = 0;
		while (!n->child[pos])
			pos++;
		return minimum(n->child[pos]);
	}
	}
}

Node **Node::findChild(const uint8_t k, Node *node) {
	switch (node->type) {
	case NodeType::N4: {
		auto n = static_cast< Node4 *>(node);
		return n->getChild(k);
	}
	case NodeType::N16: {
		auto n = static_cast< Node16 *>(node);
		return n->getChild(k);
	}
	case NodeType::N48: {
		auto n = static_cast< Node48 *>(node);
		return n->getChild(k);
	}
	case NodeType::N256: {
		auto n = static_cast< Node256 *>(node);
		return n->getChild(k);
	}
	}
	assert(false);
}


unsigned Node::prefixMismatch(bool isLittleEndian, Node *node, Key &key, size_t depth, unsigned maxKeyLength, TypeId type) {
	size_t pos;
	if (node->prefixLength > node->maxPrefixLength) {
		for (pos = 0; pos < node->maxPrefixLength; pos++)
			if (key[depth + pos] != node->prefix[pos])
				return pos;
		auto leaf = static_cast<Leaf *>(minimum(node));
		Key &minKey = *new Key(isLittleEndian, type, leaf->value);
		for (; pos < node->prefixLength; pos++)
			if (key[depth + pos] != minKey[depth + pos])
				return pos;
	} else {
		for (pos = 0; pos < node->prefixLength; pos++)
			if (key[depth + pos] != node->prefix[pos])
				return pos;
	}
	return pos;
}

void Node::insertLeaf(Node *node, Node **nodeRef, uint8_t key, Node *newNode) {
	switch (node->type) {
	case NodeType::N4:
		Node4::insert(static_cast<Node4 *>(node), nodeRef, key, newNode);
		break;
	case NodeType::N16:
		Node16::insert(static_cast<Node16 *>(node), nodeRef, key, newNode);
		break;
	case NodeType::N48:
		Node48::insert(static_cast<Node48 *>(node), nodeRef, key, newNode);
		break;
	case NodeType::N256:
		Node256::insert(static_cast<Node256 *>(node), nodeRef, key, newNode);
		break;
	}
}


