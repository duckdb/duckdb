#include "duckdb/execution/index/art/node.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/swap.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/swizzleable_pointer.hpp"
#include "duckdb/storage/storage_manager.hpp"

namespace duckdb {

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

void InternalType::Set(uint8_t *key_p, uint16_t key_size_p, ARTPointer *children_p, uint16_t children_size_p) {
	key = key_p;
	key_size = key_size_p;
	children = children_p;
	children_size = children_size_p;
}

Node::Node(NodeType type) : count(0), type(type) {
}

// LCOV_EXCL_START
idx_t Node::GetMin() {
	throw InternalException("GetMin not implemented for the specific node type.");
}

Node *Node::GetChild(ART &art, idx_t pos) {
	throw InternalException("GetChild not implemented for the specific node type.");
}

void Node::ReplaceChildPointer(idx_t pos, Node *node) {
	throw InternalException("ReplaceChildPointer not implemented for the specific node type.");
}
// LCOV_EXCL_STOP

void Node::InsertChild(Node *&node, uint8_t key_byte, Node *new_child) {
	switch (node->type) {
	case NodeType::N4:
		Node4::InsertChild(node, key_byte, new_child);
		break;
	case NodeType::N16:
		Node16::InsertChild(node, key_byte, new_child);
		break;
	case NodeType::N48:
		Node48::InsertChild(node, key_byte, new_child);
		break;
	case NodeType::N256:
		Node256::InsertChild(node, key_byte, new_child);
		break;
	default:
		throw InternalException("Unrecognized leaf type for insert");
	}
}

void Node::EraseChild(Node *&node, idx_t pos, ART &art) {
	switch (node->type) {
	case NodeType::N4: {
		Node4::EraseChild(node, pos, art);
		break;
	}
	case NodeType::N16: {
		Node16::EraseChild(node, pos, art);
		break;
	}
	case NodeType::N48: {
		Node48::EraseChild(node, pos, art);
		break;
	}
	case NodeType::N256:
		Node256::EraseChild(node, pos, art);
		break;
	default:
		throw InternalException("Unrecognized leaf type for erase");
	}
}

NodeType Node::GetTypeBySize(idx_t size) {

	if (size <= Node4::GetSize()) {
		return NodeType::N4;
	} else if (size <= Node16::GetSize()) {
		return NodeType::N16;
	} else if (size <= Node48::GetSize()) {
		return NodeType::N48;
	}
	D_ASSERT(size <= Node256::GetSize());
	return NodeType::N256;
}

void Node::New(NodeType &type, Node *&node) {
	switch (type) {
	case NodeType::N4:
		node = (Node *)Node4::New();
		return;
	case NodeType::N16:
		node = (Node *)Node16::New();
		return;
	case NodeType::N48:
		node = (Node *)Node48::New();
		return;
	case NodeType::N256:
		node = (Node *)Node256::New();
		return;
	default:
		throw InternalException("Unrecognized type for new node creation!");
	}
}

template <typename T, typename... ARGS>
T *AllocateObject(ARGS &&...args) {
	auto data = Allocator::DefaultAllocator().AllocateData(sizeof(T));
	return new (data) T(std::forward<ARGS>(args)...);
}

template <typename T>
void DestroyObject(T *ptr) {
	ptr->~T();
	Allocator::DefaultAllocator().FreeData((data_ptr_t)ptr, sizeof(T));
}

Node4 *Node4::New() {
	return AllocateObject<Node4>();
}

Node16 *Node16::New() {
	return AllocateObject<Node16>();
}

Node48 *Node48::New() {
	return AllocateObject<Node48>();
}

Node256 *Node256::New() {
	return AllocateObject<Node256>();
}

Leaf *Leaf::New(Key &value, uint32_t depth, row_t row_id) {
	return AllocateObject<Leaf>(value, depth, row_id);
}

Leaf *Leaf::New(Key &value, uint32_t depth, row_t *row_ids, idx_t num_elements) {
	return AllocateObject<Leaf>(value, depth, row_ids, num_elements);
}

Leaf *Leaf::New(unique_ptr<row_t[]> row_ids, idx_t num_elements, Prefix &prefix) {
	return AllocateObject<Leaf>(move(row_ids), num_elements, prefix);
}

Leaf *Leaf::New(row_t row_id, Prefix &prefix) {
	return AllocateObject<Leaf>(row_id, prefix);
}

void Node::Delete(Node *ptr) {
	switch (ptr->type) {
	case NodeType::NLeaf:
		DestroyObject((Leaf *)ptr);
		break;
	case NodeType::N4:
		DestroyObject((Node4 *)ptr);
		break;
	case NodeType::N16:
		DestroyObject((Node16 *)ptr);
		break;
	case NodeType::N48:
		DestroyObject((Node48 *)ptr);
		break;
	case NodeType::N256:
		DestroyObject((Node256 *)ptr);
		break;
	default:
		throw InternalException("eek");
	}
}

string Node::ToString(ART &art) {

	string str = "Node";
	switch (this->type) {
	case NodeType::NLeaf:
		return Leaf::ToString(this);
	case NodeType::N4:
		str += to_string(Node4::GetSize());
		break;
	case NodeType::N16:
		str += to_string(Node16::GetSize());
		break;
	case NodeType::N48:
		str += to_string(Node48::GetSize());
		break;
	case NodeType::N256:
		str += to_string(Node256::GetSize());
		break;
	}

	str += ": [";
	auto next_pos = GetNextPos(DConstants::INVALID_INDEX);
	while (next_pos != DConstants::INVALID_INDEX) {
		auto child = GetChild(art, next_pos);
		str += "(" + to_string(next_pos) + ", " + child->ToString(art) + ")";
		next_pos = GetNextPos(next_pos);
	}
	return str + "]";
}

BlockPointer Node::SerializeInternal(ART &art, duckdb::MetaBlockWriter &writer, InternalType &internal_type) {
	// Iterate through children and annotate their offsets
	vector<BlockPointer> child_offsets;
	for (idx_t i = 0; i < internal_type.children_size; i++) {
		child_offsets.emplace_back(internal_type.children[i].Serialize(art, writer));
	}
	auto ptr = writer.GetBlockPointer();
	// Write Node Type
	writer.Write(type);
	// Write count
	writer.Write<uint16_t>(count);
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
	return ptr;
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
		internal_type.children[i] = ARTPointer(reader);
	}
}

Node *Node::Deserialize(ART &art, idx_t block_id, idx_t offset) {
	MetaBlockReader reader(art.table_io_manager.GetIndexBlockManager(), block_id);
	reader.offset = offset;
	auto n = reader.Read<uint8_t>();
	NodeType node_type(static_cast<NodeType>(n));
	Node *deserialized_node;
	switch (node_type) {
	case NodeType::NLeaf:
		return Leaf::Deserialize(reader);
	case NodeType::N4: {
		deserialized_node = (Node *)Node4::New();
		break;
	}
	case NodeType::N16: {
		deserialized_node = (Node *)Node16::New();
		break;
	}
	case NodeType::N48: {
		deserialized_node = (Node *)Node48::New();
		break;
	}
	case NodeType::N256: {
		deserialized_node = (Node *)Node256::New();
		break;
	}
	}
	deserialized_node->DeserializeInternal(reader);
	return deserialized_node;
}

void UpdateParentsOfNodes(Node *&l_node, Node *&r_node, ParentsOfNodes &parents) {
	if (parents.l_parent) {
		parents.l_parent->ReplaceChildPointer(parents.l_pos, l_node);
	}
	if (parents.r_parent) {
		parents.r_parent->ReplaceChildPointer(parents.r_pos, r_node);
	}
}

bool Merge(MergeInfo &info, idx_t depth, ParentsOfNodes &parents) {

	// always try to merge the smaller node into the bigger node
	// because maybe there is enough free space in the bigger node to fit the smaller one
	// without too much recursion

	if (info.l_node->type < info.r_node->type) {
		// swap subtrees to ensure that l_node has the bigger node type
		swap(info.l_art, info.r_art);
		swap(info.l_node, info.r_node);
		UpdateParentsOfNodes(info.l_node, info.r_node, parents);
	}

	switch (info.r_node->type) {
	case NodeType::N256:
		return Node256::Merge(info, depth, parents.l_parent, parents.l_pos);
	case NodeType::N48:
		return Node48::Merge(info, depth, parents.l_parent, parents.l_pos);
	case NodeType::N16:
		return Node16::Merge(info, depth, parents.l_parent, parents.l_pos);
	case NodeType::N4:
		return Node4::Merge(info, depth, parents.l_parent, parents.l_pos);
	case NodeType::NLeaf:
		D_ASSERT(info.l_node->type == NodeType::NLeaf);
		D_ASSERT(info.r_node->type == NodeType::NLeaf);
		if (info.l_art->IsUnique()) {
			return false;
		}
		Leaf::Merge(info.l_node, info.r_node);
		return true;
	}
	throw InternalException("Invalid node type for right node in merge.");
}

bool ResolvePrefixesAndMerge(MergeInfo &info, idx_t depth, ParentsOfNodes &parents) {
	auto &l_node = info.l_node;
	auto &r_node = info.r_node;
	Node *null_parent = nullptr;

	// NOTE: we always merge into the left ART
	D_ASSERT(l_node);

	// make sure that r_node has the longer (or equally long) prefix
	if (l_node->prefix.Size() > r_node->prefix.Size()) {
		swap(info.l_art, info.r_art);
		swap(l_node, r_node);
		UpdateParentsOfNodes(l_node, r_node, parents);
	}

	auto mismatch_pos = l_node->prefix.MismatchPosition(r_node->prefix);

	// both nodes have no prefix or the same prefix
	if (mismatch_pos == l_node->prefix.Size() && l_node->prefix.Size() == r_node->prefix.Size()) {
		return Merge(info, depth + mismatch_pos, parents);
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

		// insert r_node as a child of l_node at empty position
		if (child_pos == DConstants::INVALID_INDEX) {
			Node::InsertChild(l_node, mismatch_byte, r_node);
			UpdateParentsOfNodes(l_node, null_parent, parents);
			r_node = nullptr;
			return true;
		}

		// recurse
		auto child_node = l_node->GetChild(*info.l_art, child_pos);
		MergeInfo child_info(info.l_art, info.r_art, child_node, r_node);
		ParentsOfNodes child_parents(l_node, child_pos, parents.r_parent, parents.r_pos);
		return ResolvePrefixesAndMerge(child_info, depth + mismatch_pos, child_parents);
	}

	// prefixes differ, create new node and insert both nodes as children

	// create new node
	Node *new_node = Node4::New();
	new_node->prefix = Prefix(l_node->prefix, mismatch_pos);

	// insert l_node, break up prefix of l_node
	auto key_byte = l_node->prefix.Reduce(mismatch_pos);
	Node4::InsertChild(new_node, key_byte, l_node);

	// insert r_node, break up prefix of r_node
	key_byte = r_node->prefix.Reduce(mismatch_pos);
	Node4::InsertChild(new_node, key_byte, r_node);

	l_node = new_node;
	UpdateParentsOfNodes(l_node, null_parent, parents);
	r_node = nullptr;
	return true;
}

bool Node::MergeAtByte(MergeInfo &info, idx_t depth, idx_t &l_child_pos, idx_t &r_pos, uint8_t &key_byte,
                       Node *&l_parent, idx_t l_pos) {

	auto r_child = info.r_node->GetChild(*info.r_art, r_pos);

	// insert child at empty position
	if (l_child_pos == DConstants::INVALID_INDEX) {
		Node::InsertChild(info.l_node, key_byte, r_child);
		if (l_parent) {
			l_parent->ReplaceChildPointer(l_pos, info.l_node);
		}
		info.r_node->ReplaceChildPointer(r_pos, nullptr);
		return true;
	}

	// recurse
	auto l_child = info.l_node->GetChild(*info.l_art, l_child_pos);
	MergeInfo child_info(info.l_art, info.r_art, l_child, r_child);
	ParentsOfNodes child_parents(info.l_node, l_child_pos, info.r_node, r_pos);
	return ResolvePrefixesAndMerge(child_info, depth + 1, child_parents);
}

bool Node::MergeARTs(ART *l_art, ART *r_art) {

	Node *null_parent = nullptr;
	MergeInfo info(l_art, r_art, l_art->tree, r_art->tree);
	ParentsOfNodes parents(null_parent, 0, null_parent, 0);
	return ResolvePrefixesAndMerge(info, 0, parents);
}

} // namespace duckdb
