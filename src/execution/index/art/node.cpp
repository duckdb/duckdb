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
	memcpy(dst->prefix.get(), src->prefix.get(), min(src->prefixLength, src->maxPrefixLength));
}

Node *Node::minimum(Node *node) {
	if (!node)
		return NULL;

	if (node->type == NodeType::NLeaf)
		return node;

	switch (node->type) {
	case NodeType::N4: {
		Node4 *n = static_cast<Node4 *>(node);
		return minimum(n->child[0].get());
	}
	case NodeType::N16: {
		Node16 *n = static_cast<Node16 *>(node);
		return minimum(n->child[0].get());
	}
	case NodeType::N48: {
		Node48 *n = static_cast<Node48 *>(node);
		unsigned pos = 0;
		while (n->childIndex[pos] == emptyMarker)
			pos++;
		return minimum(n->child[n->childIndex[pos]].get());
	}
	case NodeType::N256: {
		Node256 *n = static_cast<Node256 *>(node);
		unsigned pos = 0;
		while (!n->child[pos])
			pos++;
		return minimum(n->child[pos].get());
	}
	default:
		assert(0);
		return nullptr;
	}
}

unique_ptr<Node>* Node::findChild(const uint8_t k, unique_ptr<Node>& node) {
	switch (node->type) {
	case NodeType::N4: {
		auto n = static_cast<Node4 *>(node.get());
		return n->getChild(k);
	}
	case NodeType::N16: {
		auto n = static_cast<Node16 *>(node.get());
		return n->getChild(k);
	}
	case NodeType::N48: {
		auto n = static_cast<Node48 *>(node.get());
		return n->getChild(k);
	}
	case NodeType::N256: {
		auto n = static_cast<Node256 *>(node.get());
		return n->getChild(k);
	}
	default:
		assert(0);
		return nullptr;
	}
}

Node *Node::findChild(const uint8_t k, Node *node) {
	switch (node->type) {
		case NodeType::N4: {
			auto n = static_cast<Node4 *>(node);
			auto child = n->getChild(k);
			if (child)
				return child->get();
			else
				return nullptr;
		}
		case NodeType::N16: {
			auto n = static_cast<Node16 *>(node);
            auto child = n->getChild(k);
			if (child)
				return child->get();
			else
				return nullptr;
		}
		case NodeType::N48: {
			auto n = static_cast<Node48 *>(node);
            auto child = n->getChild(k);
			if (child)
				return child->get();
			else
				return nullptr;
		}
		case NodeType::N256: {
			auto n = static_cast<Node256 *>(node);
            auto child = n->getChild(k);
			if (child)
            	return child->get();
			else
				return nullptr;
		}
		default:
			assert(0);
			return nullptr;
	}
}

unsigned Node::prefixMismatch(bool isLittleEndian, Node *node, Key &key, uint64_t depth, unsigned maxKeyLength,
                              TypeId type) {
	uint64_t pos;
	if (node->prefixLength > node->maxPrefixLength) {
		for (pos = 0; pos < node->maxPrefixLength; pos++)
			if (key[depth + pos] != node->prefix[pos])
				return pos;
		auto leaf = static_cast<Leaf *>(minimum(node));
		Key &minKey = *new Key(isLittleEndian, type, leaf->value,maxKeyLength);
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

void Node::insertLeaf(unique_ptr<Node>& node, uint8_t key,unique_ptr<Node>&  newNode) {
	switch (node->type) {
	case NodeType::N4:
		Node4::insert(node, key, newNode);
		break;
	case NodeType::N16:
		Node16::insert(node, key, newNode);
		break;
	case NodeType::N48:
		Node48::insert(node, key, newNode);
		break;
	case NodeType::N256:
		Node256::insert(node, key, newNode);
		break;
	default:
		assert(0);
	}
}
