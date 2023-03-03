#include "duckdb/execution/index/art/art_node.hpp"

#include "duckdb/storage/table_io_manager.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Constructors / Destructors
//===--------------------------------------------------------------------===//

ARTNode::ARTNode() : SwizzleablePointer() {
}

ARTNode::ARTNode(MetaBlockReader &reader) : SwizzleablePointer(reader) {
}

ARTNode ARTNode::New(ART &art, const ARTNodeType &type) {

	ARTNode node;
	D_ASSERT(art.nodes.find(type) != art.nodes.end());
	node.pointer = art.nodes.at(type).New();
	node.EncodeARTNodeType(type);
	return node;
}

void ARTNode::Free(ART &art, ARTNode &node) {

	// recursively free all nodes that are in-memory, and skip swizzled nodes
	D_ASSERT(node);

	if (!node.IsSwizzled()) {

		node.GetPrefix(art)->Free(art);

		auto position = node.pointer & 0x0fffffff;
		auto type = node.DecodeARTNodeType();

		// free the children of the node
		switch (type) {
		case ARTNodeType::LEAF:
			Leaf::Free(art, node);
			break;
		case ARTNodeType::NODE_4:
			Node4::Free(art, node);
			break;
		case ARTNodeType::NODE_16:
			Node16::Free(art, node);
			break;
		case ARTNodeType::NODE_48:
			Node48::Free(art, node);
			break;
		case ARTNodeType::NODE_256:
			Node256::Free(art, node);
			break;
		default:
			throw InternalException("Invalid node type for Delete.");
		}

		// free the node itself
		D_ASSERT(art.nodes.find(type) != art.nodes.end());
		return art.nodes.at(type).Free(position);
	}

	// just overwrite with an empty ART node, if a swizzled pointer
	node = ARTNode();
}

void ARTNode::Initialize(ART &art, ARTNode &node, const ARTNodeType &type) {

	node = ARTNode::New(art, type);

	switch (type) {
	case ARTNodeType::NODE_4:
		Node4::Initialize(art, node);
		break;
	case ARTNodeType::NODE_16:
		Node16::Initialize(art, node);
		break;
	case ARTNodeType::NODE_48:
		Node48::Initialize(art, node);
		break;
	case ARTNodeType::NODE_256:
		Node256::Initialize(art, node);
		break;
	default:
		throw InternalException("Invalid node type for Initialize.");
	}
}

//===--------------------------------------------------------------------===//
// Vacuum
//===--------------------------------------------------------------------===//

void ARTNode::Vacuum(ART &art, ARTNode &node, const unordered_set<ARTNodeType, ARTNodeTypeHash> &vacuum_nodes) {

	if (node.IsSwizzled()) {
		return;
	}

	// possibly vacuum prefix
	if (vacuum_nodes.find(ARTNodeType::PREFIX_SEGMENT) != vacuum_nodes.end()) {
		node.GetPrefix(art)->Vacuum(art);
	}

	auto type = node.DecodeARTNodeType();
	if (vacuum_nodes.find(type) != vacuum_nodes.end()) {
		D_ASSERT(art.nodes.find(type) != art.nodes.end());
		auto position = node.pointer & 0x0fffffff;
		if (art.nodes.at(type).NeedsVacuum(position)) {
			node.pointer = art.nodes.at(type).Vacuum(position);
			node.EncodeARTNodeType(type);
		}
	}

	switch (node.DecodeARTNodeType()) {
	case ARTNodeType::LEAF: {
		if (vacuum_nodes.find(ARTNodeType::LEAF_SEGMENT) != vacuum_nodes.end()) {
			node.Get<Leaf>(art)->Vacuum(art);
		}
		return;
	}
	case ARTNodeType::NODE_4:
		return node.Get<Node4>(art)->Vacuum(art, vacuum_nodes);
	case ARTNodeType::NODE_16: {
		return node.Get<Node16>(art)->Vacuum(art, vacuum_nodes);
	case ARTNodeType::NODE_48:
		return node.Get<Node48>(art)->Vacuum(art, vacuum_nodes);
	case ARTNodeType::NODE_256:
		return node.Get<Node256>(art)->Vacuum(art, vacuum_nodes);
	default:
		throw InternalException("Invalid node type for Vacuum.");
	}
	}
}

//===--------------------------------------------------------------------===//
// Get
//===--------------------------------------------------------------------===//

template <class T>
T *ARTNode::Get(ART &art) const {

	auto type = DecodeARTNodeType();
	D_ASSERT(art.nodes.find(type) != art.nodes.end());
	return art.nodes.at(type).Get<T>(pointer & 0x0fffffff);
}

//===--------------------------------------------------------------------===//
// Encoding / Decoding the node type
//===--------------------------------------------------------------------===//

void ARTNode::EncodeARTNodeType(const ARTNodeType &type) {

	// left shift the type by 7 bytes
	auto type_64_bit = (idx_t)type;
	type_64_bit <<= ((sizeof(idx_t) - sizeof(uint8_t)) * 8);

	// ensure that we do not overwrite any bits
	D_ASSERT(pointer >> ((sizeof(idx_t) - sizeof(uint8_t)) * 8) == 0);
	pointer &= type_64_bit;
}

ARTNodeType ARTNode::DecodeARTNodeType() const {

	// right shift by 7 bytes
	auto type = pointer >> ((sizeof(idx_t) - sizeof(uint8_t)) * 8);
	return ARTNodeType(type);
}

//===--------------------------------------------------------------------===//
// Inserts
//===--------------------------------------------------------------------===//

void ARTNode::ReplaceChild(ART &art, const idx_t &pos, ARTNode &child) {

	D_ASSERT(!IsSwizzled());

	switch (DecodeARTNodeType()) {
	case ARTNodeType::NODE_4:
		return Get<Node4>(art)->ReplaceChild(pos, child);
	case ARTNodeType::NODE_16:
		return Get<Node16>(art)->ReplaceChild(pos, child);
	case ARTNodeType::NODE_48:
		return Get<Node48>(art)->ReplaceChild(pos, child);
	case ARTNodeType::NODE_256:
		return Get<Node256>(art)->ReplaceChild(pos, child);
	default:
		throw InternalException("Invalid node type for ReplaceChild.");
	}
}

void ARTNode::InsertChild(ART &art, ARTNode &node, const uint8_t &byte, ARTNode &child) {

	switch (node.DecodeARTNodeType()) {
	case ARTNodeType::NODE_4:
		return Node4::InsertChild(art, node, byte, child);
	case ARTNodeType::NODE_16:
		return Node16::InsertChild(art, node, byte, child);
	case ARTNodeType::NODE_48:
		return Node48::InsertChild(art, node, byte, child);
	case ARTNodeType::NODE_256:
		return Node256::InsertChild(art, node, byte, child);
	default:
		throw InternalException("Invalid node type for InsertChild.");
	}
}

//===--------------------------------------------------------------------===//
// Deletes
//===--------------------------------------------------------------------===//

void ARTNode::DeleteChild(ART &art, ARTNode &node, idx_t pos) {

	switch (node.DecodeARTNodeType()) {
	case ARTNodeType::NODE_4:
		return Node4::DeleteChild(art, node, pos);
	case ARTNodeType::NODE_16:
		return Node16::DeleteChild(art, node, pos);
	case ARTNodeType::NODE_48:
		return Node48::DeleteChild(art, node, pos);
	case ARTNodeType::NODE_256:
		return Node256::DeleteChild(art, node, pos);
	default:
		throw InternalException("Invalid node type for DeleteChild.");
	}
}

//===--------------------------------------------------------------------===//
// Get functions
//===--------------------------------------------------------------------===//

ARTNode ARTNode::GetChild(ART &art, const idx_t &pos) const {

	D_ASSERT(!IsSwizzled());

	ARTNode child;
	switch (DecodeARTNodeType()) {
	case ARTNodeType::NODE_4: {
		child = Get<Node4>(art)->GetChild(pos);
		break;
	}
	case ARTNodeType::NODE_16: {
		child = Get<Node16>(art)->GetChild(pos);
		break;
	}
	case ARTNodeType::NODE_48: {
		child = Get<Node48>(art)->GetChild(pos);
		break;
	}
	case ARTNodeType::NODE_256: {
		child = Get<Node256>(art)->GetChild(pos);
		break;
	}
	default:
		throw InternalException("Invalid node type for GetChild.");
	}

	// unswizzle the ART node before returning it
	if (child.IsSwizzled()) {
		auto block = child.GetBlockInfo();
		child.Deserialize(art, block.block_id, block.offset);
	}
	return child;
}

uint8_t ARTNode::GetKeyByte(ART &art, const idx_t &pos) const {

	D_ASSERT(!IsSwizzled());

	switch (DecodeARTNodeType()) {
	case ARTNodeType::NODE_4:
		return Get<Node4>(art)->GetKeyByte(pos);
	case ARTNodeType::NODE_16:
		return Get<Node16>(art)->GetKeyByte(pos);
	case ARTNodeType::NODE_48:
		return Get<Node48>(art)->GetKeyByte(pos);
	case ARTNodeType::NODE_256:
		return Get<Node256>(art)->GetKeyByte(pos);
	default:
		throw InternalException("Invalid node type for GetKeyByte.");
	}
}

idx_t ARTNode::GetChildPos(ART &art, const uint8_t &byte) const {

	D_ASSERT(!IsSwizzled());

	switch (DecodeARTNodeType()) {
	case ARTNodeType::NODE_4:
		return Get<Node4>(art)->GetChildPos(byte);
	case ARTNodeType::NODE_16:
		return Get<Node16>(art)->GetChildPos(byte);
	case ARTNodeType::NODE_48:
		return Get<Node48>(art)->GetChildPos(byte);
	case ARTNodeType::NODE_256:
		return Get<Node256>(art)->GetChildPos(byte);
	default:
		throw InternalException("Invalid node type for GetChild.");
	}
}

idx_t ARTNode::GetChildPosGreaterEqual(ART &art, const uint8_t &byte, bool &inclusive) const {

	D_ASSERT(!IsSwizzled());

	switch (DecodeARTNodeType()) {
	case ARTNodeType::NODE_4:
		return Get<Node4>(art)->GetChildPosGreaterEqual(byte, inclusive);
	case ARTNodeType::NODE_16:
		return Get<Node16>(art)->GetChildPosGreaterEqual(byte, inclusive);
	case ARTNodeType::NODE_48:
		return Get<Node48>(art)->GetChildPosGreaterEqual(byte, inclusive);
	case ARTNodeType::NODE_256:
		return Get<Node256>(art)->GetChildPosGreaterEqual(byte, inclusive);
	default:
		throw InternalException("Invalid node type for GetChildPosGreaterEqual.");
	}
}

idx_t ARTNode::GetMinPos(ART &art) const {

	D_ASSERT(!IsSwizzled());

	switch (DecodeARTNodeType()) {
	case ARTNodeType::NODE_4:
		return Get<Node4>(art)->GetMinPos();
	case ARTNodeType::NODE_16:
		return Get<Node16>(art)->GetMinPos();
	case ARTNodeType::NODE_48:
		return Get<Node48>(art)->GetMinPos();
	case ARTNodeType::NODE_256:
		return Get<Node256>(art)->GetMinPos();
	default:
		throw InternalException("Invalid node type for GetMinPos.");
	}
}

idx_t ARTNode::GetNextPos(ART &art, idx_t pos) const {

	D_ASSERT(!IsSwizzled());

	switch (DecodeARTNodeType()) {
	case ARTNodeType::NODE_4:
		return Get<Node4>(art)->GetNextPos(pos);
	case ARTNodeType::NODE_16:
		return Get<Node16>(art)->GetNextPos(pos);
	case ARTNodeType::NODE_48:
		return Get<Node48>(art)->GetNextPos(pos);
	case ARTNodeType::NODE_256:
		return Get<Node256>(art)->GetNextPos(pos);
	default:
		throw InternalException("Invalid node type for GetNextPos.");
	}
}

idx_t ARTNode::GetNextPosAndByte(ART &art, idx_t pos, uint8_t &byte) const {

	D_ASSERT(!IsSwizzled());

	switch (DecodeARTNodeType()) {
	case ARTNodeType::NODE_4:
		return Get<Node4>(art)->GetNextPosAndByte(pos, byte);
	case ARTNodeType::NODE_16:
		return Get<Node16>(art)->GetNextPosAndByte(pos, byte);
	case ARTNodeType::NODE_48:
		return Get<Node48>(art)->GetNextPosAndByte(pos, byte);
	case ARTNodeType::NODE_256:
		return Get<Node256>(art)->GetNextPosAndByte(pos, byte);
	default:
		throw InternalException("Invalid node type for GetNextPosAndByte.");
	}
}

//===--------------------------------------------------------------------===//
// (De)serialization
//===--------------------------------------------------------------------===//

BlockPointer ARTNode::Serialize(ART &art, MetaBlockWriter &writer) {

	if (!*this) {
		return {(block_id_t)DConstants::INVALID_INDEX, (uint32_t)DConstants::INVALID_INDEX};
	}

	if (IsSwizzled()) {
		auto block = GetBlockInfo();
		Deserialize(art, block.block_id, block.offset);
	}

	switch (DecodeARTNodeType()) {
	case ARTNodeType::LEAF:
		return Get<Leaf>(art)->Serialize(art, writer);
	case ARTNodeType::NODE_4:
		return Get<Node4>(art)->Serialize(art, writer);
	case ARTNodeType::NODE_16:
		return Get<Node16>(art)->Serialize(art, writer);
	case ARTNodeType::NODE_48:
		return Get<Node48>(art)->Serialize(art, writer);
	case ARTNodeType::NODE_256:
		return Get<Node256>(art)->Serialize(art, writer);
	default:
		throw InternalException("Invalid node type for Serialize.");
	}
}

void ARTNode::Deserialize(ART &art, idx_t block_id, idx_t offset) {

	MetaBlockReader reader(art.table_io_manager.GetIndexBlockManager(), block_id);
	reader.offset = offset;

	auto type_byte = reader.Read<uint8_t>();
	ARTNodeType type((ARTNodeType)(type_byte));
	*this = ARTNode::New(art, type);

	switch (type) {
	case ARTNodeType::LEAF:
		return Get<Leaf>(art)->Deserialize(art, reader);
	case ARTNodeType::NODE_4:
		return Get<Node4>(art)->Deserialize(art, reader);
	case ARTNodeType::NODE_16:
		return Get<Node16>(art)->Deserialize(art, reader);
	case ARTNodeType::NODE_48:
		return Get<Node48>(art)->Deserialize(art, reader);
	case ARTNodeType::NODE_256:
		return Get<Node256>(art)->Deserialize(art, reader);
	default:
		throw InternalException("Invalid node type for Deserialize.");
	}
}

//===--------------------------------------------------------------------===//
// Utility
//===--------------------------------------------------------------------===//

string ARTNode::ToString(ART &art) const {

	D_ASSERT(!IsSwizzled());

	if (DecodeARTNodeType() == ARTNodeType::LEAF) {
		return Get<Leaf>(art)->ToString(art);
	}

	string str = "Node" + to_string(GetCapacity()) + ": [";

	auto next_pos = GetNextPos(art, DConstants::INVALID_INDEX);
	while (next_pos != DConstants::INVALID_INDEX) {
		auto child = GetChild(art, next_pos);
		str += "(" + to_string(next_pos) + ", " + child.ToString(art) + ")";
		next_pos = GetNextPos(art, next_pos);
	}

	return str + "]";
}

idx_t ARTNode::GetCapacity() const {

	D_ASSERT(!IsSwizzled());

	switch (DecodeARTNodeType()) {
	case ARTNodeType::NODE_4:
		return ARTNode::NODE_4_CAPACITY;
	case ARTNodeType::NODE_16:
		return ARTNode::NODE_16_CAPACITY;
	case ARTNodeType::NODE_48:
		return ARTNode::NODE_48_CAPACITY;
	case ARTNodeType::NODE_256:
		return ARTNode::NODE_256_CAPACITY;
	default:
		throw InternalException("Invalid node type for GetCapacity.");
	}
}

Prefix *ARTNode::GetPrefix(ART &art) const {

	D_ASSERT(!IsSwizzled());

	switch (DecodeARTNodeType()) {
	case ARTNodeType::LEAF:
		return &Get<Leaf>(art)->prefix;
	case ARTNodeType::NODE_4:
		return &Get<Node4>(art)->prefix;
	case ARTNodeType::NODE_16:
		return &Get<Node16>(art)->prefix;
	case ARTNodeType::NODE_48:
		return &Get<Node48>(art)->prefix;
	case ARTNodeType::NODE_256:
		return &Get<Node256>(art)->prefix;
	default:
		throw InternalException("Invalid node type for GetPrefix.");
	}
}

ARTNodeType ARTNode::GetARTNodeTypeByCount(const idx_t &count) {

	if (count <= NODE_4_CAPACITY) {
		return ARTNodeType::NODE_4;
	} else if (count <= NODE_16_CAPACITY) {
		return ARTNodeType::NODE_16;
	} else if (count <= NODE_48_CAPACITY) {
		return ARTNodeType::NODE_48;
	}
	return ARTNodeType::NODE_256;
}

//===--------------------------------------------------------------------===//
// Merging
//===--------------------------------------------------------------------===//

// forward declaration
bool ResolvePrefixesAndMerge(MergeInfo &info, ParentsOfARTNodes &parents);

void UpdateParentsOfNodes(ARTNode *&l_node, ARTNode *&r_node, ParentsOfARTNodes &parents) {
	// TODO
	//	if (parents.l_parent) {
	//		parents.l_parent->ReplaceChildPointer(parents.l_pos, l_node);
	//	}
	//	if (parents.r_parent) {
	//		parents.r_parent->ReplaceChildPointer(parents.r_pos, r_node);
	//	}
}

void SwapNodes(MergeInfo &info, ParentsOfARTNodes &parents) {
	// TODO
	//
	//	// actual swap
	//	swap(info.l_art, info.r_art);
	//	swap(info.l_node, info.r_node);
	//	UpdateParentsOfNodes(info.l_node, info.r_node, parents);
}

bool Merge(MergeInfo &info, ParentsOfARTNodes &parents) {
	// TODO
	//
	//	D_ASSERT(info.l_node);
	//	D_ASSERT(info.r_node);
	//
	//	// always try to merge the smaller node into the bigger node
	//	// because maybe there is enough free space in the bigger node to fit the smaller one
	//	// without too much recursion
	//
	//	if (info.l_node->type < info.r_node->type) {
	//		// swap subtrees to ensure that l_node has the bigger node type
	//		SwapNodes(info, parents);
	//	}
	//
	//	if (info.r_node->type == NodeType::LEAF) {
	//		D_ASSERT(info.l_node->type == NodeType::LEAF);
	//		D_ASSERT(info.r_node->type == NodeType::LEAF);
	//		if (info.l_art->IsUnique()) {
	//			return false;
	//		}
	//		Leaf::Merge(*info.root_l_art, info.l_node, info.r_node);
	//		return true;
	//	}
	//
	//	uint8_t key_byte;
	//	idx_t r_child_pos = DConstants::INVALID_INDEX;
	//
	//	while (true) {
	//		r_child_pos = info.r_node->GetNextPosAndByte(r_child_pos, key_byte);
	//		if (r_child_pos == DConstants::INVALID_INDEX) {
	//			break;
	//		}
	//		auto r_child = info.r_node->GetChild(*info.r_art, r_child_pos);
	//		auto l_child_pos = info.l_node->GetChildPos(key_byte);
	//
	//		if (l_child_pos == DConstants::INVALID_INDEX) {
	//			// insert child at empty position
	//			auto r_memory_size = r_child->MemorySize(*info.r_art, true);
	//			Node::InsertChild(*info.root_l_art, info.l_node, key_byte, r_child);
	//
	//			info.root_l_art->IncreaseMemorySize(r_memory_size);
	//			info.root_r_art->DecreaseMemorySize(r_memory_size);
	//			if (parents.l_parent) {
	//				parents.l_parent->ReplaceChildPointer(parents.l_pos, info.l_node);
	//			}
	//			info.r_node->ReplaceChildPointer(r_child_pos, nullptr);
	//
	//		} else {
	//			// recurse
	//			auto l_child = info.l_node->GetChild(*info.l_art, l_child_pos);
	//			MergeInfo child_info(info.l_art, info.r_art, info.root_l_art, info.root_r_art, l_child, r_child);
	//			ParentsOfNodes child_parents(info.l_node, l_child_pos, info.r_node, r_child_pos);
	//			if (!ResolvePrefixesAndMerge(child_info, child_parents)) {
	//				return false;
	//			}
	//		}
	//	}
	//	return true;
}

bool ResolvePrefixesAndMerge(MergeInfo &info, ParentsOfARTNodes &parents) {
	// TODO
	//	// NOTE: we always merge into the left ART
	//
	//	D_ASSERT(info.l_node);
	//	D_ASSERT(info.r_node);
	//
	//	// make sure that r_node has the longer (or equally long) prefix
	//	if (info.l_node->prefix.Size() > info.r_node->prefix.Size()) {
	//		SwapNodes(info, parents);
	//	}
	//
	//	Node *null_parent = nullptr;
	//	auto &l_node = info.l_node;
	//	auto &r_node = info.r_node;
	//	auto l_prefix_size = l_node->prefix.Size();
	//	auto r_prefix_size = r_node->prefix.Size();
	//
	//	auto mismatch_pos = l_node->prefix.MismatchPosition(r_node->prefix);
	//
	//	// both nodes have no prefix or the same prefix
	//	if (mismatch_pos == l_prefix_size && l_prefix_size == r_prefix_size) {
	//		return Merge(info, parents);
	//	}
	//
	//	if (mismatch_pos == l_prefix_size) {
	//		// r_node's prefix contains l_node's prefix
	//		// l_node cannot be a leaf, otherwise the key represented by l_node would be a subset of another key
	//		// which is not possible by our construction
	//		D_ASSERT(l_node->type != NodeType::LEAF);
	//
	//		// test if the next byte (mismatch_pos) in r_node (longer prefix) exists in l_node
	//		auto mismatch_byte = r_node->prefix[mismatch_pos];
	//		auto child_pos = l_node->GetChildPos(mismatch_byte);
	//
	//		// update the prefix of r_node to only consist of the bytes after mismatch_pos
	//		r_node->prefix.Reduce(*info.root_r_art, mismatch_pos);
	//
	//		// insert r_node as a child of l_node at empty position
	//		if (child_pos == DConstants::INVALID_INDEX) {
	//
	//			auto r_memory_size = r_node->MemorySize(*info.r_art, true);
	//			Node::InsertChild(*info.root_l_art, l_node, mismatch_byte, r_node);
	//
	//			info.root_l_art->IncreaseMemorySize(r_memory_size);
	//			info.root_r_art->DecreaseMemorySize(r_memory_size);
	//			UpdateParentsOfNodes(l_node, null_parent, parents);
	//			r_node = nullptr;
	//			return true;
	//		}
	//
	//		// recurse
	//		auto child_node = l_node->GetChild(*info.l_art, child_pos);
	//		MergeInfo child_info(info.l_art, info.r_art, info.root_l_art, info.root_r_art, child_node, r_node);
	//		ParentsOfNodes child_parents(l_node, child_pos, parents.r_parent, parents.r_pos);
	//		return ResolvePrefixesAndMerge(child_info, child_parents);
	//	}
	//
	//	// prefixes differ, create new node and insert both nodes as children
	//
	//	// create new node
	//	Node *new_node = NODE_4:New();
	//	new_node->prefix = Prefix(l_node->prefix, mismatch_pos);
	//	info.root_l_art->IncreaseMemorySize(new_node->MemorySize(*info.l_art, false));
	//
	//	// insert l_node, break up prefix of l_node
	//	auto key_byte = l_node->prefix.Reduce(*info.root_l_art, mismatch_pos);
	//	NODE_4:InsertChild(*info.root_l_art, new_node, key_byte, l_node);
	//
	//	// insert r_node, break up prefix of r_node
	//	key_byte = r_node->prefix.Reduce(*info.root_r_art, mismatch_pos);
	//	auto r_memory_size = r_node->MemorySize(*info.r_art, true);
	//	NODE_4:InsertChild(*info.root_l_art, new_node, key_byte, r_node);
	//
	//	info.root_l_art->IncreaseMemorySize(r_memory_size);
	//	info.root_r_art->DecreaseMemorySize(r_memory_size);
	//
	//	l_node = new_node;
	//	UpdateParentsOfNodes(l_node, null_parent, parents);
	//	r_node = nullptr;
	//	return true;
}

bool ARTNode::MergeARTs(ART *l_art, ART *r_art) {
	// TODO
	//	Node *null_parent = nullptr;
	//	MergeInfo info(l_art, r_art, l_art, r_art, l_art->tree, r_art->tree);
	//	ParentsOfNodes parents(null_parent, 0, null_parent, 0);
	//	return ResolvePrefixesAndMerge(info, parents);
}

//===--------------------------------------------------------------------===//
// Memory tracking (DEBUG)
//===--------------------------------------------------------------------===//

bool ARTNode::InMemory() {
	return *this && !IsSwizzled();
}

idx_t ARTNode::MemorySize(ART &art, const bool &recurse) {
#ifdef DEBUG

	if (IsSwizzled()) {
		return 0;
	}

	// if recurse, then get the memory size of the subtree
	auto memory_size_children = 0;
	if (recurse) {
		auto next_pos = GetNextPos(art, DConstants::INVALID_INDEX);
		while (next_pos != DConstants::INVALID_INDEX) {
			if (ChildIsInMemory(art, next_pos)) {
				auto child = GetChild(art, next_pos);
				memory_size_children += child.MemorySize(art, recurse);
			}
			next_pos = GetNextPos(art, next_pos);
		}
	}

	// get the memory size of the node itself
	auto type = DecodeARTNodeType();
	switch (type) {
	case ARTNodeType::NODE_4:
		// TODO
	case ARTNodeType::NODE_16:
		// TODO
	case ARTNodeType::NODE_48:
		// TODO
	case ARTNodeType::NODE_256:
		// TODO
	default:
		throw InternalException("Invalid node type for MemorySize.");
	}

#endif
}

bool ARTNode::ChildIsInMemory(ART &art, const idx_t &pos) {
#ifdef DEBUG

	D_ASSERT(!IsSwizzled());

	auto type = DecodeARTNodeType();
	switch (type) {
	case ARTNodeType::NODE_4:
		// TODO
	case ARTNodeType::NODE_16:
		// TODO
	case ARTNodeType::NODE_48:
		// TODO
	case ARTNodeType::NODE_256:
		// TODO
	default:
		throw InternalException("Invalid node type for ChildIsInMemory.");
	}

#endif
}

} // namespace duckdb
