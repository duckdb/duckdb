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

void InternalType::Set(uint8_t *key_p, uint16_t key_size_p, SwizzleablePointer *children_p, uint16_t children_size_p) {
	key = key_p;
	key_size = key_size_p;
	children = children_p;
	children_size = children_size_p;
}

InternalType::InternalType(Node *n) {
	switch (n->type) {
	case NodeType::N4: {
		auto n4 = (Node4 *)n;
		Set(n4->key, 4, n4->children, 4);
		break;
	}
	case NodeType::N16: {
		auto n16 = (Node16 *)n;
		Set(n16->key, 16, n16->children, 16);
		break;
	}
	case NodeType::N48: {
		auto n48 = (Node48 *)n;
		Set(n48->child_index, 256, n48->children, 48);
		break;
	}
	case NodeType::N256: {
		auto n256 = (Node256 *)n;
		Set(nullptr, 0, n256->children, 256);
		break;
	}
	default:
		throw InternalException("This is not an Internal ART Node Type");
	}
}

BlockPointer Node::SerializeInternal(ART &art, duckdb::MetaBlockWriter &writer, InternalType &internal_type) {
	// Iterate through children and annotate their offsets
	vector<BlockPointer> child_offsets;
	for (idx_t i = 0; i < internal_type.children_size; i++) {
		child_offsets.emplace_back(internal_type.children[i].Serialize(art, writer));
	}
	auto block_id = writer.block->id;
	uint32_t offset = writer.offset;
	// Write Node Type
	writer.Write(type);
	// Write compression Info
	writer.Write(prefix_length);
	// Write count
	writer.Write(count);

	for (idx_t i = 0; i < prefix_length; i++) {
		writer.Write(prefix[i]);
	}
	// Write Key values
	for (idx_t i = 0; i < internal_type.key_size; i++) {
		writer.Write(internal_type.key[i]);
	}
	// Write child offsets
	for (auto &offsets : child_offsets) {
		writer.Write(offsets.block_id);
		writer.Write(offsets.offset);
	}
	return {block_id, offset};
}

BlockPointer Node::Serialize(ART &art, duckdb::MetaBlockWriter &writer) {
	switch (type) {
	case NodeType::N4:
	case NodeType::N16:
	case NodeType::N48:
	case NodeType::N256: {
		InternalType internal_type(this);
		return SerializeInternal(art, writer, internal_type);
	}
	case NodeType::NLeaf: {
		auto leaf = (Leaf *)this;
		return leaf->SerializeLeaf(writer);
	}
	default:
		throw InternalException("Invalid ART Node");
	}
}

void Node::DeserializeInternal(duckdb::MetaBlockReader &reader, uint32_t prefix_length_p) {
	InternalType internal_type(this);
	count = reader.Read<uint16_t>();
	prefix_length = prefix_length_p;
	for (idx_t i = 0; i < prefix_length; i++) {
		prefix[i] = reader.Read<uint8_t>();
	}
	// Get Key values
	for (idx_t i = 0; i < internal_type.key_size; i++) {
		internal_type.key[i] = reader.Read<uint8_t>();
	}
	// Get Child offsets
	for (idx_t i = 0; i < internal_type.children_size; i++) {
		internal_type.children[i] = SwizzleablePointer(reader);
	}
}

Node *Node::Deserialize(ART &art, idx_t block_id, idx_t offset) {
	MetaBlockReader reader(art.db, block_id);
	reader.offset = offset;
	auto n = reader.Read<uint8_t>();
	NodeType node_type(static_cast<NodeType>(n));
	auto prefix_value_length = reader.Read<uint32_t>();
	Node *deserialized_node;
	switch (node_type) {
	case NodeType::NLeaf:
		return Leaf::Deserialize(reader, prefix_value_length);
	case NodeType::N4: {
		deserialized_node = new Node4(prefix_value_length);
		break;
	}
	case NodeType::N16: {
		deserialized_node = new Node16(prefix_value_length);
		break;
	}
	case NodeType::N48: {
		deserialized_node = new Node48(prefix_value_length);
		break;
	}
	case NodeType::N256: {
		deserialized_node = new Node256(prefix_value_length);
		break;
	}
	}
	deserialized_node->DeserializeInternal(reader, prefix_value_length);
	return deserialized_node;
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

void Node::Erase(Node *&node, idx_t pos, ART &art) {
	switch (node->type) {
	case NodeType::N4: {
		Node4::Erase(node, pos, art);
		break;
	}
	case NodeType::N16: {
		Node16::Erase(node, pos, art);
		break;
	}
	case NodeType::N48: {
		Node48::Erase(node, pos, art);
		break;
	}
	case NodeType::N256:
		Node256::Erase(node, pos, art);
		break;
	default:
		throw InternalException("Unrecognized leaf type for erase");
	}
}

SwizzleablePointer::~SwizzleablePointer() {
	if (pointer) {
		if (!IsSwizzled()) {
			delete (Node *)pointer;
		}
	}
}

SwizzleablePointer::SwizzleablePointer(duckdb::MetaBlockReader &reader) {
	idx_t block_id = reader.Read<block_id_t>();
	idx_t offset = reader.Read<uint32_t>();
	if (block_id == DConstants::INVALID_INDEX || offset == DConstants::INVALID_INDEX) {
		pointer = 0;
		return;
	}
	idx_t pointer_size = sizeof(pointer) * 8;
	pointer = block_id;
	pointer = pointer << (pointer_size / 2);
	pointer += offset;
	// Set the left most bit to indicate this is a swizzled pointer and send it back to the mother-ship
	uint64_t mask = 1;
	mask = mask << (pointer_size - 1);
	pointer |= mask;
}

SwizzleablePointer &SwizzleablePointer::operator=(const Node *ptr) {
	if (sizeof(ptr) == 4) {
		pointer = (uint32_t)(size_t)ptr;
	} else {
		pointer = (uint64_t)ptr;
	}
	return *this;
}

bool operator!=(const SwizzleablePointer &s_ptr, const uint64_t &ptr) {
	return (s_ptr.pointer != ptr);
}

BlockPointer SwizzleablePointer::GetSwizzledBlockInfo() {
	D_ASSERT(IsSwizzled());
	idx_t pointer_size = sizeof(pointer) * 8;
	pointer = pointer & ~(1ULL << (pointer_size - 1));
	uint32_t block_id = pointer >> (pointer_size / 2);
	uint32_t offset = pointer & 0xffffffff;
	return {block_id, offset};
}
bool SwizzleablePointer::IsSwizzled() {
	idx_t pointer_size = sizeof(pointer) * 8;
	return (pointer >> (pointer_size - 1)) & 1;
}

void SwizzleablePointer::Reset() {
	if (pointer) {
		if (!IsSwizzled()) {
			delete (Node *)pointer;
		}
	}
	*this = nullptr;
}

Node *SwizzleablePointer::Unswizzle(ART &art) {
	if (IsSwizzled()) {
		// This means our pointer is not yet in memory, gotta deserialize this
		// first we unset the bae
		auto block_info = GetSwizzledBlockInfo();
		*this = Node::Deserialize(art, block_info.block_id, block_info.offset);
	}
	return (Node *)pointer;
}

BlockPointer SwizzleablePointer::Serialize(ART &art, duckdb::MetaBlockWriter &writer) {
	if (pointer) {
		Unswizzle(art);
		return ((Node *)pointer)->Serialize(art, writer);
	} else {
		return {(block_id_t)DConstants::INVALID_INDEX, (uint32_t)DConstants::INVALID_INDEX};
	}
}

void Node::ResolvePrefixesAndMerge(Node *l_node, Node *r_node) {

	// make sure that this node has a shorter or equal prefix length than other_node
	if (l_node->prefix_length > r_node->prefix_length) {
		Node::ResolvePrefixesAndMerge(r_node, l_node);
		return;
	}

	// get first mismatch position
	auto mismatch_pos = l_node->prefix_length;
	for (idx_t i = 0; i < l_node->prefix_length; i++) {
		if (l_node->prefix[i] != r_node->prefix[i]) {
			mismatch_pos = i;
			break;
		}
	}

	// no prefix or same prefix
	if (mismatch_pos == l_node->prefix_length && l_node->prefix_length == r_node->prefix_length) {
		Node::Merge(l_node, r_node);

	} else if (mismatch_pos == l_node->prefix_length) {
		// other_node's prefix contains this node's prefix

		// update the prefix of r_node to only contain the bytes after mismatch_pos
		r_node->prefix_length -= (mismatch_pos + 1);
		memmove(r_node->prefix.get(), r_node->prefix.get() + mismatch_pos + 1, r_node->prefix_length);

		// test if the next byte in r_node (longer prefix) exists in l_node
		auto child_pos = l_node->GetChildPos(r_node->prefix[mismatch_pos]);
		if (child_pos == DConstants::INVALID_INDEX) {
			// TODO: set r_node as the child of l_node
			// TODO: for this we need to have a reference to the ART in this function (for pointer swizzling)
		} else {
			// TODO: merge the child of l_node at child_pos with r_node (ResolvePrefixesAndMerge)
		}

	} else {
		// prefixes differ, create new node and insert both nodes as children

		// create new node
		Node *new_node = new Node4(mismatch_pos);
		new_node->prefix_length = mismatch_pos;
		memcpy(new_node->prefix.get(), l_node->prefix.get(), mismatch_pos);

		// insert l_node, break up prefix of l_node
		Node4::Insert(new_node, l_node->prefix[mismatch_pos], l_node);
		l_node->prefix_length -= (mismatch_pos + 1);
		memmove(l_node->prefix.get(), l_node->prefix.get() + mismatch_pos + 1, l_node->prefix_length);

		// insert r_node, break up prefix of r_node
		Node4::Insert(new_node, r_node->prefix[mismatch_pos], r_node);
		r_node->prefix_length -= (mismatch_pos + 1);
		memmove(r_node->prefix.get(), r_node->prefix.get() + mismatch_pos + 1, r_node->prefix_length);

		l_node = new_node;
	}
}

void Merge(Node *l_node, Node *r_node) {

	// make sure that the smaller node is l_node
	if (l_node->type > r_node->type) {
		Merge(r_node, l_node);
		return;
	}

	switch (l_node->type) {
	case NodeType::NLeaf: {
		// leaf - leaf
		// leaf - n4
		// leaf - n16
		// leaf - n48
		// leaf - n256
		Leaf::Merge(l_node, r_node);
		break;
	}
	case NodeType::N4: {
		// n4 - n4
		// n4 - n16
		// n4 - n48
		// n4 - n256
		Node4::Merge(l_node, r_node);
		break;
	}
	case NodeType::N16: {
		// n16 - n16
		// n16 - n48
		// n16 - n256
		Node16::Merge(l_node, r_node);
		break;
	}
	case NodeType::N48: {
		// n48 - n48
		// n48 - n256
		Node48::Merge(l_node, r_node);
		break;
	}
	case NodeType::N256: {
		// n256 - n256
		Node256::Merge(l_node, r_node);
		break;
	}
	}
}

} // namespace duckdb
