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
Node *Node::GetChild(ART &art, idx_t pos) {
	D_ASSERT(0);
	return nullptr;
}

void Node::ReplaceChildPointer(idx_t pos, Node *node) {
	D_ASSERT(0);
}

idx_t Node::GetMin() {
	D_ASSERT(0);
	return 0;
}
// LCOV_EXCL_STOP

Node *Node::GetChildSwizzled(ART &art, uintptr_t pointer) {
	idx_t pointer_size = sizeof(pointer) * 8;
	// We first check if left-most bit is set
	bool in_disk = (pointer >> (pointer_size - 1)) & 1;
	if (in_disk) {
		// This means our pointer is not yet in memory, gotta deserialize this
		// first we unset the bae
		pointer = pointer & ~(1UL << pointer_size);
		uint32_t block_id = pointer >> (pointer_size / 2);
		uint32_t offset = pointer & 0xffffffff;
		return Deserialize(art, block_id, offset);

	} else {
		return (Node *)pointer;
	}
}

uintptr_t Node::GenerateSwizzledPointer(uint32_t block_id, uint32_t offset) {
	uintptr_t pointer;
	idx_t pointer_size = sizeof(pointer) * 8;
	pointer = block_id;
	pointer = pointer << (pointer_size / 2);
	pointer += offset;
	// Set the left most bit to indicate this is a swizzled pointer and send it back to the mother-ship
	return pointer << ((pointer_size)-1);
}

Node *Node::Deserialize(ART &art, idx_t block_id, idx_t offset) {
	MetaBlockReader reader(art.db, block_id);
	reader.offset = offset;
	NodeType node_type(static_cast<NodeType>(reader.Read<uint8_t>()));
	switch (node_type) {
	case NodeType::NLeaf:
		return Leaf::Deserialize(reader);
	case NodeType::N4:
		return Node4::Deserialize(reader);
	case NodeType::N16:
		return Node16::Deserialize(reader);
	case NodeType::N48:
		return Node48::Deserialize(reader);
	case NodeType::N256:
		return Node256::Deserialize(reader);
	default:
		throw InternalException("Invalid ART Node Type");
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

void Node::InsertLeaf(Node *&node, uint8_t key, Node *new_node) {
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

void Node::Erase(Node *&node, idx_t pos) {
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
