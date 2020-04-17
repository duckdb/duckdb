#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/common/exception.hpp"

using namespace duckdb;

Node::Node(ART &art, NodeType type, size_t compressedPrefixSize) : prefix_length(0), count(0), type(type) {
	this->prefix = unique_ptr<uint8_t[]>(new uint8_t[compressedPrefixSize]);
}

void Node::CopyPrefix(ART &art, Node *src, Node *dst) {
	dst->prefix_length = src->prefix_length;
	memcpy(dst->prefix.get(), src->prefix.get(), src->prefix_length);
}

unique_ptr<Node> *Node::GetChild(idx_t pos) {
	assert(0);
	return nullptr;
}

idx_t Node::GetMin() {
	assert(0);
	return 0;
}

uint32_t Node::PrefixMismatch(ART &art, Node *node, Key &key, uint64_t depth) {
	uint64_t pos;
	for (pos = 0; pos < node->prefix_length; pos++) {
		if (key[depth + pos] != node->prefix[pos]) {
			return pos;
		}
	}
	return pos;
}

void Node::InsertLeaf(ART &art, unique_ptr<Node> &node, uint8_t key, unique_ptr<Node> &newNode) {
	switch (node->type) {
	case NodeType::N4:
		Node4::insert(art, node, key, newNode);
		break;
	case NodeType::N16:
		Node16::insert(art, node, key, newNode);
		break;
	case NodeType::N48:
		Node48::insert(art, node, key, newNode);
		break;
	case NodeType::N256:
		Node256::insert(art, node, key, newNode);
		break;
	default:
		assert(0);
	}
}

void Node::Erase(ART &art, unique_ptr<Node> &node, idx_t pos) {
	switch (node->type) {
	case NodeType::N4: {
		Node4::erase(art, node, pos);
		break;
	}
	case NodeType::N16: {
		Node16::erase(art, node, pos);
		break;
	}
	case NodeType::N48: {
		Node48::erase(art, node, pos);
		break;
	}
	case NodeType::N256:
		Node256::erase(art, node, pos);
		break;
	default:
		assert(0);
		break;
	}
}
