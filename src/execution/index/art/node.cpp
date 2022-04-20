#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

Node::Node(NodeType type, size_t compressed_prefix_size) : prefix_length(0), count(0), type(type) {
	this->prefix = unique_ptr<uint8_t[]>(new uint8_t[compressed_prefix_size]);
}

void Node::CopyPrefix(Node *src, Node *dst) {
	dst->prefix_length = src->prefix_length;
	memcpy(dst->prefix.get(), src->prefix.get(), src->prefix_length);
}

// LCOV_EXCL_START
unique_ptr<Node> *Node::GetChild(idx_t pos) {
	D_ASSERT(0);
	return nullptr;
}

idx_t Node::GetMin() {
	D_ASSERT(0);
	return 0;
}
// LCOV_EXCL_STOP

static unique_ptr<Node> Deserialize(duckdb::Deserializer &source, std::pair<idx_t, idx_t> offsets) {
	auto node_type = source.Read<idx_t>();
	switch (node_type) {
	case 0:
		return Leaf::Deserialize(source, offsets);
	case 4:
	case 16:
	case 48:
	case 256:
	default:
		throw InternalException("This ART node type does not exist");
	}
}

uint32_t Node::PrefixMismatch(Node *node, Key &key, uint64_t depth) {
	uint64_t pos;
	for (pos = 0; pos < node->prefix_length; pos++) {
		if (key[depth + pos] != node->prefix[pos]) {
			return pos;
		}
	}
	return pos;
}

void Node::InsertLeaf(unique_ptr<Node> &node, uint8_t key, unique_ptr<Node> &new_node) {
	switch (node->type) {
	case NodeType::N4:
		Node4::Insert(node, key, new_node);
		break;
	case NodeType::N16:
		Node16::Insert(node, key, new_node);
		break;
	case NodeType::N48:
		Node48::Insert(node, key, new_node);
		break;
	case NodeType::N256:
		Node256::Insert(node, key, new_node);
		break;
	default:
		throw InternalException("Unrecognized leaf type for insert");
	}
}

void Node::Erase(unique_ptr<Node> &node, idx_t pos) {
	switch (node->type) {
	case NodeType::N4: {
		Node4::Erase(node, pos);
		break;
	}
	case NodeType::N16: {
		Node16::Erase(node, pos);
		break;
	}
	case NodeType::N48: {
		Node48::Erase(node, pos);
		break;
	}
	case NodeType::N256:
		Node256::Erase(node, pos);
		break;
	default:
		throw InternalException("Unrecognized leaf type for erase");
	}
}

} // namespace duckdb
