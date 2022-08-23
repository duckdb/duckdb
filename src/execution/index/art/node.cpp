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

void Node::InsertChildNode(Node *&node, uint8_t key_byte, Node *new_child) {
	switch (node->type) {
	case NodeType::N4:
		Node4::Insert(node, key_byte, new_child);
		break;
	case NodeType::N16:
		Node16::Insert(node, key_byte, new_child);
		break;
	case NodeType::N48:
		Node48::Insert(node, key_byte, new_child);
		break;
	case NodeType::N256:
		Node256::Insert(node, key_byte, new_child);
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

void SwapNodes(Node *&l_node, Node *&r_node) {
	auto l_node_temp = l_node;
	l_node = r_node;
	r_node = l_node_temp;
}

void Node::ResolvePrefixesAndMerge(ART &l_art, ART &r_art, Node *&l_node, Node *&r_node, idx_t depth,
                                   Node *&l_node_parent, idx_t l_node_pos, Node *&r_node_parent, idx_t r_node_pos) {

	if (!l_node) {
		l_node = r_node;
		if (r_node_parent) {
			r_node_parent->ReplaceChildPointer(r_node_pos, nullptr);
		}
		r_node = nullptr;
		return;
	}

	// leaves have no prefixes
	if (l_node->type == NodeType::NLeaf || r_node->type == NodeType::NLeaf) {
		return Node::Merge(r_art, l_art, l_node, r_node, depth, l_node_parent, l_node_pos, r_node_parent, r_node_pos);
	}

	// make sure that r_node has the longer (or equally long) prefix
	if (l_node->prefix_length > r_node->prefix_length) {
		SwapNodes(l_node, r_node);
		return Node::ResolvePrefixesAndMerge(r_art, l_art, l_node, r_node, depth, l_node_parent, l_node_pos,
		                                     r_node_parent, r_node_pos);
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
		return Node::Merge(l_art, r_art, l_node, r_node, depth + mismatch_pos, l_node_parent, l_node_pos, r_node_parent,
		                   r_node_pos);
	}

	if (mismatch_pos == l_node->prefix_length) {
		// r_node's prefix contains l_node's prefix

		// update the prefix of r_node to only consist of the bytes after mismatch_pos
		r_node->prefix_length -= (mismatch_pos + 1);
		memmove(r_node->prefix.get(), r_node->prefix.get() + mismatch_pos + 1, r_node->prefix_length);

		// test if the next byte (mismatch_pos) in r_node (longer prefix) exists in l_node
		auto child_pos = l_node->GetChildPos(r_node->prefix[mismatch_pos]);
		if (child_pos == DConstants::INVALID_INDEX) {
			Node::InsertChildNode(l_node, r_node->prefix[mismatch_pos], r_node);
			if (l_node_parent) {
				l_node_parent->ReplaceChildPointer(l_node_pos, l_node);
			}
			if (r_node_parent) {
				r_node_parent->ReplaceChildPointer(r_node_pos, nullptr);
			}
			r_node = nullptr;
			return;
		}

		auto child_node = l_node->GetChild(l_art, child_pos);
		return Node::ResolvePrefixesAndMerge(l_art, r_art, child_node, r_node, depth + mismatch_pos, l_node, child_pos,
		                                     r_node_parent, r_node_pos);
	}

	// prefixes differ, create new node and insert both nodes as children

	// create new node
	Node *new_node = new Node4(mismatch_pos - 1);
	new_node->prefix_length = mismatch_pos - 1;
	memcpy(new_node->prefix.get(), l_node->prefix.get(), mismatch_pos - 1);

	// insert l_node, break up prefix of l_node
	Node4::Insert(new_node, l_node->prefix[mismatch_pos], l_node);
	l_node->prefix_length -= (mismatch_pos + 1);
	memmove(l_node->prefix.get(), l_node->prefix.get() + mismatch_pos + 1, l_node->prefix_length);

	// insert r_node, break up prefix of r_node
	Node4::Insert(new_node, r_node->prefix[mismatch_pos], r_node);
	r_node->prefix_length -= (mismatch_pos + 1);
	memmove(r_node->prefix.get(), r_node->prefix.get() + mismatch_pos + 1, r_node->prefix_length);

	l_node = new_node;
	if (l_node_parent) {
		l_node_parent->ReplaceChildPointer(l_node_pos, l_node);
	}
	if (r_node_parent) {
		r_node_parent->ReplaceChildPointer(r_node_pos, nullptr);
	}
	r_node = nullptr;
}

void Node::Merge(ART &l_art, ART &r_art, Node *&l_node, Node *&r_node, idx_t depth, Node *&l_node_parent,
                 idx_t l_node_pos, Node *&r_node_parent, idx_t r_node_pos) {

	// always try to merge the smaller node into the bigger node
	// because maybe there is enough free space in the bigger node to fit the smaller one
	// without too much recursion

	if (l_node->type < r_node->type) {
		// swap subtrees to ensure that l_node has the bigger node type
		SwapNodes(l_node, r_node);
		return Node::Merge(r_art, l_art, l_node, r_node, depth, l_node_parent, l_node_pos, r_node_parent, r_node_pos);
	}

	switch (r_node->type) {
	case NodeType::N256:
		return Node::MergeNodeWithNode256(l_art, r_art, l_node, r_node, depth, l_node_parent, l_node_pos);
	case NodeType::N48:
		return Node::MergeNodeWithNode48(l_art, r_art, l_node, r_node, depth, l_node_parent, l_node_pos);
	case NodeType::N16:
		return Node::MergeNodeWithNode16OrNode4<Node16>(l_art, r_art, l_node, r_node, depth, l_node_parent, l_node_pos);
	case NodeType::N4:
		return Node::MergeNodeWithNode16OrNode4<Node4>(l_art, r_art, l_node, r_node, depth, l_node_parent, l_node_pos);
	case NodeType::NLeaf:
		return Leaf::Merge(l_art, r_art, l_node, r_node, depth, r_node_parent, r_node_pos, l_node_parent, l_node_pos);
	}
	throw InternalException("Invalid node type for right node in merge.");
}

template <class R_NODE_TYPE>
void Node::MergeNodeWithNode16OrNode4(ART &l_art, ART &r_art, Node *&l_node, Node *&r_node, idx_t depth,
                                      Node *&l_node_parent, idx_t l_node_pos) {

	R_NODE_TYPE *r_n = (R_NODE_TYPE *)r_node;

	for (idx_t i = 0; i < r_node->count; i++) {

		auto l_child_pos = l_node->GetChildPos(r_n->key[i]);
		Node::MergeByte(l_art, r_art, l_node, r_node, depth, l_child_pos, i, r_n->key[i], l_node_parent, l_node_pos);
	}
}

void Node::MergeNodeWithNode48(ART &l_art, ART &r_art, Node *&l_node, Node *&r_node, idx_t depth, Node *&l_node_parent,
                               idx_t l_node_pos) {

	Node48 *r_n = (Node48 *)r_node;

	for (idx_t i = 0; i < 256; i++) {
		if (r_n->child_index[i] != Node::EMPTY_MARKER) {

			auto l_child_pos = l_node->GetChildPos(i);
			auto key_byte = (uint8_t)i;
			Node::MergeByte(l_art, r_art, l_node, r_node, depth, l_child_pos, i, key_byte, l_node_parent, l_node_pos);
		}
	}
}

void Node::MergeNodeWithNode256(ART &l_art, ART &r_art, Node *&l_node, Node *&r_node, idx_t depth, Node *&l_node_parent,
                                idx_t l_node_pos) {

	for (idx_t i = 0; i < 256; i++) {
		if (r_node->GetChildPos(i) != DConstants::INVALID_INDEX) {

			auto l_child_pos = l_node->GetChildPos(i);
			auto key_byte = (uint8_t)i;
			Node::MergeByte(l_art, r_art, l_node, r_node, depth, l_child_pos, i, key_byte, l_node_parent, l_node_pos);
		}
	}
}

void Node::MergeByte(ART &l_art, ART &r_art, Node *&l_node, Node *&r_node, idx_t depth, idx_t &l_child_pos,
                     idx_t &r_pos, uint8_t &key_byte, Node *&l_node_parent, idx_t l_node_pos) {

	auto r_child = r_node->GetChild(r_art, r_pos);
	if (l_child_pos == DConstants::INVALID_INDEX) {
		Node::InsertChildNode(l_node, key_byte, r_child);
		if (l_node_parent) {
			l_node_parent->ReplaceChildPointer(l_node_pos, l_node);
		}
		r_node->ReplaceChildPointer(r_pos, nullptr);
		return;
	}
	// recurse
	auto l_child = l_node->GetChild(l_art, l_child_pos);
	Node::ResolvePrefixesAndMerge(l_art, r_art, l_child, r_child, depth + 1, l_node, l_child_pos, r_node, r_pos);
}

} // namespace duckdb
