#include "execution/index/art/node.hpp"
#include "execution/index/art/node4.hpp"
#include "execution/index/art/node16.hpp"
#include "execution/index/art/node48.hpp"
#include "execution/index/art/node256.hpp"
#include "common/exception.hpp"

using namespace duckdb;

unsigned Node::min(unsigned a, unsigned b) {
	return (a < b) ? a : b;
}

void Node::copyPrefix(Node *src, Node *dst) {
	dst->prefix_length = src->prefix_length;
	memcpy(dst->prefix.get(), src->prefix.get(), min(src->prefix_length, src->max_prefix_length));
}

unique_ptr<Node> *Node::minimum(unique_ptr<Node> &node) {
	if (!node)
		return nullptr;

	if (node->type == NodeType::NLeaf) {
		return &node;
	}

	switch (node->type) {
	case NodeType::N4: {
		Node4 *n = static_cast<Node4 *>(node.get());
		auto min_val = n->getMin();
		return minimum(*min_val);
	}
	case NodeType::N16: {
		Node16 *n = static_cast<Node16 *>(node.get());
		auto min_val = n->getMin();
		return minimum(*min_val);
	}
	case NodeType::N48: {
		Node48 *n = static_cast<Node48 *>(node.get());
		auto min_val = n->getMin();
		return minimum(*min_val);
	}
	case NodeType::N256: {
		Node256 *n = static_cast<Node256 *>(node.get());
		auto min_val = n->getMin();
		return minimum(*min_val);
	}
	default:
		assert(0);
		return nullptr;
	}
}

unique_ptr<Node> *Node::findChild(const uint8_t k, unique_ptr<Node> &node) {
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

int Node::findKeyPos(const uint8_t k, Node *node) {
	switch (node->type) {
	case NodeType::N4: {
		auto n = static_cast<Node4 *>(node);
		return n->getPos(k);
	}
	case NodeType::N16: {
		auto n = static_cast<Node16 *>(node);
		return n->getPos(k);
	}
	case NodeType::N48: {
		auto n = static_cast<Node48 *>(node);
		return n->getPos(k);
	}
	case NodeType::N256: {
		auto n = static_cast<Node256 *>(node);
		return n->getPos(k);
	}
	default:
		assert(0);
		return -1;
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
	// TODO: node->prefix_length > node->max_prefix_length
	if (node->prefix_length <= node->max_prefix_length) {
		for (pos = 0; pos < node->prefix_length; pos++)
			if (key[depth + pos] != node->prefix[pos])
				return pos;
	} else {
		throw NotImplementedException("Operation not implemented");
	}
	return pos;
}

void Node::insertLeaf(unique_ptr<Node> &node, uint8_t key, unique_ptr<Node> &newNode) {
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
