#include "duckdb/execution/index/art/node.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/execution/index/art/swizzleable_pointer.hpp"

namespace duckdb {

Node::Node(NodeType type) : count(0), type(type) {
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
	// Write count
	writer.Write(count);
	// Write Prefix
	prefix.Serialize(writer);
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
		return leaf->Serialize(writer);
	}
	default:
		throw InternalException("Invalid ART Node");
	}
}

void Node::DeserializeInternal(duckdb::MetaBlockReader &reader) {
	InternalType internal_type(this);
	count = reader.Read<uint16_t>();
	prefix.Deserialize(reader);
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
	Node *deserialized_node;
	switch (node_type) {
	case NodeType::NLeaf:
		return Leaf::Deserialize(reader);
	case NodeType::N4: {
		deserialized_node = (Node *)new Node4();
		break;
	}
	case NodeType::N16: {
		deserialized_node = (Node *)new Node16();
		break;
	}
	case NodeType::N48: {
		deserialized_node = (Node *)new Node48();
		break;
	}
	case NodeType::N256: {
		deserialized_node = (Node *)new Node256();
		break;
	}
	}
	deserialized_node->DeserializeInternal(reader);
	return deserialized_node;
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

void Node::ResolvePrefixesAndMerge(ART *l_art, ART *r_art, Node *&l_node, Node *&r_node, idx_t depth,
                                   Node *&l_node_parent, idx_t l_node_pos, Node *&r_node_parent, idx_t r_node_pos) {

	if (!l_node) {
		l_node = r_node;
		if (l_node_parent) {
			l_node_parent->ReplaceChildPointer(l_node_pos, l_node);
		}
		if (r_node_parent) {
			r_node_parent->ReplaceChildPointer(r_node_pos, nullptr);
		}
		r_node = nullptr;
		return;
	}

	// make sure that r_node has the longer (or equally long) prefix
	if (l_node->prefix.Size() > r_node->prefix.Size()) {

		auto temp_art = l_art;
		l_art = r_art;
		r_art = temp_art;

		auto temp_node = l_node;
		l_node = r_node;
		r_node = temp_node;

		if (l_node_parent) {
			l_node_parent->ReplaceChildPointer(l_node_pos, l_node);
		}
		if (r_node_parent) {
			r_node_parent->ReplaceChildPointer(r_node_pos, r_node);
		}
	}

	// get first mismatch position
	auto mismatch_pos = l_node->prefix.Size();
	for (idx_t i = 0; i < l_node->prefix.Size(); i++) {
		if (l_node->prefix[i] != r_node->prefix[i]) {
			mismatch_pos = i;
			break;
		}
	}

	// no prefix or same prefix
	if (mismatch_pos == l_node->prefix.Size() && l_node->prefix.Size() == r_node->prefix.Size()) {
		return Node::Merge(l_art, r_art, l_node, r_node, depth + mismatch_pos, l_node_parent, l_node_pos, r_node_parent,
		                   r_node_pos);
	}

	if (mismatch_pos == l_node->prefix.Size()) {
		// r_node's prefix contains l_node's prefix
		// l_node cannot be a leaf, otherwise the key represented by l_node would be a subset of another key
		// which is not possible by our construction
		D_ASSERT(l_node->type != NodeType::NLeaf);

		// test if the next byte (mismatch_pos) in r_node (longer prefix) exists in l_node
		auto mismatch_byte = r_node->prefix[mismatch_pos];
		auto child_pos = l_node->GetChildPos(mismatch_byte);

		// update the prefix of r_node to only consist of the bytes after mismatch_pos
		r_node->prefix.Reduce(mismatch_pos);

		if (child_pos == DConstants::INVALID_INDEX) {
			Node::InsertChildNode(l_node, mismatch_byte, r_node);
			if (l_node_parent) {
				l_node_parent->ReplaceChildPointer(l_node_pos, l_node);
			}
			if (r_node_parent) {
				r_node_parent->ReplaceChildPointer(r_node_pos, nullptr);
			}
			r_node = nullptr;
			return;
		}

		auto child_node = l_node->GetChild(*l_art, child_pos);
		return Node::ResolvePrefixesAndMerge(l_art, r_art, child_node, r_node, depth + mismatch_pos, l_node, child_pos,
		                                     r_node_parent, r_node_pos);
	}

	// prefixes differ, create new node and insert both nodes as children

	// create new node
	Node *new_node = new Node4();
	new_node->prefix = Prefix(l_node->prefix, mismatch_pos);

	// insert l_node, break up prefix of l_node
	auto key_byte = l_node->prefix.Reduce(mismatch_pos);
	Node4::Insert(new_node, key_byte, l_node);

	// insert r_node, break up prefix of r_node
	key_byte = r_node->prefix.Reduce(mismatch_pos);
	Node4::Insert(new_node, key_byte, r_node);

	l_node = new_node;
	if (l_node_parent) {
		l_node_parent->ReplaceChildPointer(l_node_pos, l_node);
	}
	if (r_node_parent) {
		r_node_parent->ReplaceChildPointer(r_node_pos, nullptr);
	}
	r_node = nullptr;
}

void Node::Merge(ART *l_art, ART *r_art, Node *&l_node, Node *&r_node, idx_t depth, Node *&l_node_parent,
                 idx_t l_node_pos, Node *&r_node_parent, idx_t r_node_pos) {

	// always try to merge the smaller node into the bigger node
	// because maybe there is enough free space in the bigger node to fit the smaller one
	// without too much recursion

	if (l_node->type < r_node->type) {
		// swap subtrees to ensure that l_node has the bigger node type

		auto temp_art = l_art;
		l_art = r_art;
		r_art = temp_art;

		auto temp_node = l_node;
		l_node = r_node;
		r_node = temp_node;

		if (l_node_parent) {
			l_node_parent->ReplaceChildPointer(l_node_pos, l_node);
		}
		if (r_node_parent) {
			r_node_parent->ReplaceChildPointer(r_node_pos, r_node);
		}
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
		D_ASSERT(l_node->type == NodeType::NLeaf);
		D_ASSERT(r_node->type == NodeType::NLeaf);
		return Leaf::Merge((l_art->IsPrimary() || l_art->IsUnique()), l_node, r_node);
	}
	throw InternalException("Invalid node type for right node in merge.");
}

template <class R_NODE_TYPE>
void Node::MergeNodeWithNode16OrNode4(ART *l_art, ART *r_art, Node *&l_node, Node *&r_node, idx_t depth,
                                      Node *&l_node_parent, idx_t l_node_pos) {

	R_NODE_TYPE *r_n = (R_NODE_TYPE *)r_node;

	for (idx_t i = 0; i < r_node->count; i++) {

		auto l_child_pos = l_node->GetChildPos(r_n->key[i]);
		Node::MergeByte(l_art, r_art, l_node, r_node, depth, l_child_pos, i, r_n->key[i], l_node_parent, l_node_pos);
	}
}

void Node::MergeNodeWithNode48(ART *l_art, ART *r_art, Node *&l_node, Node *&r_node, idx_t depth, Node *&l_node_parent,
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

void Node::MergeNodeWithNode256(ART *l_art, ART *r_art, Node *&l_node, Node *&r_node, idx_t depth, Node *&l_node_parent,
                                idx_t l_node_pos) {

	for (idx_t i = 0; i < 256; i++) {
		if (r_node->GetChildPos(i) != DConstants::INVALID_INDEX) {

			auto l_child_pos = l_node->GetChildPos(i);
			auto key_byte = (uint8_t)i;
			Node::MergeByte(l_art, r_art, l_node, r_node, depth, l_child_pos, i, key_byte, l_node_parent, l_node_pos);
		}
	}
}

void Node::MergeByte(ART *l_art, ART *r_art, Node *&l_node, Node *&r_node, idx_t depth, idx_t &l_child_pos,
                     idx_t &r_pos, uint8_t &key_byte, Node *&l_node_parent, idx_t l_node_pos) {

	auto r_child = r_node->GetChild(*r_art, r_pos);
	if (l_child_pos == DConstants::INVALID_INDEX) {
		Node::InsertChildNode(l_node, key_byte, r_child);
		if (l_node_parent) {
			l_node_parent->ReplaceChildPointer(l_node_pos, l_node);
		}
		r_node->ReplaceChildPointer(r_pos, nullptr);
		return;
	}
	// recurse
	auto l_child = l_node->GetChild(*l_art, l_child_pos);
	Node::ResolvePrefixesAndMerge(l_art, r_art, l_child, r_child, depth + 1, l_node, l_child_pos, r_node, r_pos);
}

} // namespace duckdb
