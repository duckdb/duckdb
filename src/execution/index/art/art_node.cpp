#include "duckdb/execution/index/art/art_node.hpp"

#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/prefix.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/node4.hpp"
#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/node48.hpp"
#include "duckdb/execution/index/art/node256.hpp"
#include "duckdb/storage/meta_block_writer.hpp"
#include "duckdb/storage/meta_block_reader.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Constructors / Destructors
//===--------------------------------------------------------------------===//

ARTNode::ARTNode() : SwizzleablePointer() {
}

ARTNode::ARTNode(MetaBlockReader &reader) : SwizzleablePointer(reader) {
}

void ARTNode::New(ART &art, ARTNode &node, const ARTNodeType &type) {

	switch (type) {
	case ARTNodeType::NODE_4:
		Node4::New(art, node);
		break;
	case ARTNodeType::NODE_16:
		Node16::New(art, node);
		break;
	case ARTNodeType::NODE_48:
		Node48::New(art, node);
		break;
	case ARTNodeType::NODE_256:
		Node256::New(art, node);
		break;
	default:
		throw InternalException("Invalid node type for Initialize.");
	}
}

void ARTNode::Free(ART &art, ARTNode &node) {

	// recursively free all nodes that are in-memory, and skip swizzled and empty nodes
	if (!node) {
		return;
	}

	if (!node.IsSwizzled()) {

		node.GetPrefix(art)->Free(art);

		// free the children of the node
		auto position = node.pointer & FixedSizeAllocator::FIRST_BYTE_TO_ZERO;
		switch (node.DecodeARTNodeType()) {
		case ARTNodeType::LEAF:
			Leaf::Free(art, node);
			art.leaves->Free(position);
			break;
		case ARTNodeType::NODE_4:
			Node4::Free(art, node);
			art.n4_nodes->Free(position);
			break;
		case ARTNodeType::NODE_16:
			Node16::Free(art, node);
			art.n16_nodes->Free(position);
			break;
		case ARTNodeType::NODE_48:
			Node48::Free(art, node);
			art.n48_nodes->Free(position);
			break;
		case ARTNodeType::NODE_256:
			Node256::Free(art, node);
			art.n256_nodes->Free(position);
			break;
		default:
			throw InternalException("Invalid node type for Delete.");
		}
	}

	// overwrite with an empty ART node
	node.Reset();
}

//===--------------------------------------------------------------------===//
// Inserts
//===--------------------------------------------------------------------===//

void ARTNode::ReplaceChild(ART &art, const idx_t &position, ARTNode &child) {

	D_ASSERT(!IsSwizzled());

	switch (DecodeARTNodeType()) {
	case ARTNodeType::NODE_4:
		return art.n4_nodes->Get<Node4>(GetPtr())->ReplaceChild(position, child);
	case ARTNodeType::NODE_16:
		return art.n16_nodes->Get<Node16>(GetPtr())->ReplaceChild(position, child);
	case ARTNodeType::NODE_48:
		return art.n48_nodes->Get<Node48>(GetPtr())->ReplaceChild(position, child);
	case ARTNodeType::NODE_256:
		return art.n256_nodes->Get<Node256>(GetPtr())->ReplaceChild(position, child);
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

void ARTNode::DeleteChild(ART &art, ARTNode &node, idx_t position) {

	switch (node.DecodeARTNodeType()) {
	case ARTNodeType::NODE_4:
		return Node4::DeleteChild(art, node, position);
	case ARTNodeType::NODE_16:
		return Node16::DeleteChild(art, node, position);
	case ARTNodeType::NODE_48:
		return Node48::DeleteChild(art, node, position);
	case ARTNodeType::NODE_256:
		return Node256::DeleteChild(art, node, position);
	default:
		throw InternalException("Invalid node type for DeleteChild.");
	}
}

//===--------------------------------------------------------------------===//
// Get functions
//===--------------------------------------------------------------------===//

ARTNode *ARTNode::GetChild(ART &art, const idx_t &position) const {

	D_ASSERT(!IsSwizzled());

	ARTNode *child;
	switch (DecodeARTNodeType()) {
	case ARTNodeType::NODE_4: {
		child = art.n4_nodes->Get<Node4>(GetPtr())->GetChild(position);
		break;
	}
	case ARTNodeType::NODE_16: {
		child = art.n16_nodes->Get<Node16>(GetPtr())->GetChild(position);
		break;
	}
	case ARTNodeType::NODE_48: {
		child = art.n48_nodes->Get<Node48>(GetPtr())->GetChild(position);
		break;
	}
	case ARTNodeType::NODE_256: {
		child = art.n256_nodes->Get<Node256>(GetPtr())->GetChild(position);
		break;
	}
	default:
		throw InternalException("Invalid node type for GetChild.");
	}

	// unswizzle the ART node before returning it
	if (child->IsSwizzled()) {
		auto block = child->GetBlockInfo();
		child->Deserialize(art, block.block_id, block.offset);
	}
	return child;
}

uint8_t ARTNode::GetKeyByte(ART &art, const idx_t &position) const {

	D_ASSERT(!IsSwizzled());

	switch (DecodeARTNodeType()) {
	case ARTNodeType::NODE_4:
		return art.n4_nodes->Get<Node4>(GetPtr())->GetKeyByte(position);
	case ARTNodeType::NODE_16:
		return art.n16_nodes->Get<Node16>(GetPtr())->GetKeyByte(position);
	case ARTNodeType::NODE_48:
		return art.n48_nodes->Get<Node48>(GetPtr())->GetKeyByte(position);
	case ARTNodeType::NODE_256:
		return art.n256_nodes->Get<Node256>(GetPtr())->GetKeyByte(position);
	default:
		throw InternalException("Invalid node type for GetKeyByte.");
	}
}

idx_t ARTNode::GetChildPosition(ART &art, const uint8_t &byte) const {

	D_ASSERT(!IsSwizzled());

	switch (DecodeARTNodeType()) {
	case ARTNodeType::NODE_4:
		return art.n4_nodes->Get<Node4>(GetPtr())->GetChildPosition(byte);
	case ARTNodeType::NODE_16:
		return art.n16_nodes->Get<Node16>(GetPtr())->GetChildPosition(byte);
	case ARTNodeType::NODE_48:
		return art.n48_nodes->Get<Node48>(GetPtr())->GetChildPosition(byte);
	case ARTNodeType::NODE_256:
		return art.n256_nodes->Get<Node256>(GetPtr())->GetChildPosition(byte);
	default:
		throw InternalException("Invalid node type for GetChildPosition.");
	}
}

idx_t ARTNode::GetChildPositionGreaterEqual(ART &art, const uint8_t &byte, bool &inclusive) const {

	D_ASSERT(!IsSwizzled());

	switch (DecodeARTNodeType()) {
	case ARTNodeType::NODE_4:
		return art.n4_nodes->Get<Node4>(GetPtr())->GetChildPositionGreaterEqual(byte, inclusive);
	case ARTNodeType::NODE_16:
		return art.n16_nodes->Get<Node16>(GetPtr())->GetChildPositionGreaterEqual(byte, inclusive);
	case ARTNodeType::NODE_48:
		return art.n48_nodes->Get<Node48>(GetPtr())->GetChildPositionGreaterEqual(byte, inclusive);
	case ARTNodeType::NODE_256:
		return art.n256_nodes->Get<Node256>(GetPtr())->GetChildPositionGreaterEqual(byte, inclusive);
	default:
		throw InternalException("Invalid node type for GetChildPositionGreaterEqual.");
	}
}

idx_t ARTNode::GetMinPosition(ART &art) const {

	D_ASSERT(!IsSwizzled());

	switch (DecodeARTNodeType()) {
	case ARTNodeType::NODE_4:
		return art.n4_nodes->Get<Node4>(GetPtr())->GetMinPosition();
	case ARTNodeType::NODE_16:
		return art.n16_nodes->Get<Node16>(GetPtr())->GetMinPosition();
	case ARTNodeType::NODE_48:
		return art.n48_nodes->Get<Node48>(GetPtr())->GetMinPosition();
	case ARTNodeType::NODE_256:
		return art.n256_nodes->Get<Node256>(GetPtr())->GetMinPosition();
	default:
		throw InternalException("Invalid node type for GetMinPosition.");
	}
}

idx_t ARTNode::GetNextPosition(ART &art, idx_t position) const {

	D_ASSERT(!IsSwizzled());

	switch (DecodeARTNodeType()) {
	case ARTNodeType::NODE_4:
		return art.n4_nodes->Get<Node4>(GetPtr())->GetNextPosition(position);
	case ARTNodeType::NODE_16:
		return art.n16_nodes->Get<Node16>(GetPtr())->GetNextPosition(position);
	case ARTNodeType::NODE_48:
		return art.n48_nodes->Get<Node48>(GetPtr())->GetNextPosition(position);
	case ARTNodeType::NODE_256:
		return art.n256_nodes->Get<Node256>(GetPtr())->GetNextPosition(position);
	default:
		throw InternalException("Invalid node type for GetNextPosition.");
	}
}

idx_t ARTNode::GetNextPositionAndByte(ART &art, idx_t position, uint8_t &byte) const {

	D_ASSERT(!IsSwizzled());

	switch (DecodeARTNodeType()) {
	case ARTNodeType::NODE_4:
		return art.n4_nodes->Get<Node4>(GetPtr())->GetNextPositionAndByte(position, byte);
	case ARTNodeType::NODE_16:
		return art.n16_nodes->Get<Node16>(GetPtr())->GetNextPositionAndByte(position, byte);
	case ARTNodeType::NODE_48:
		return art.n48_nodes->Get<Node48>(GetPtr())->GetNextPositionAndByte(position, byte);
	case ARTNodeType::NODE_256:
		return art.n256_nodes->Get<Node256>(GetPtr())->GetNextPositionAndByte(position, byte);
	default:
		throw InternalException("Invalid node type for GetNextPositionAndByte.");
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
		return art.leaves->Get<Leaf>(GetPtr())->Serialize(art, writer);
	case ARTNodeType::NODE_4:
		return art.n4_nodes->Get<Node4>(GetPtr())->Serialize(art, writer);
	case ARTNodeType::NODE_16:
		return art.n16_nodes->Get<Node16>(GetPtr())->Serialize(art, writer);
	case ARTNodeType::NODE_48:
		return art.n48_nodes->Get<Node48>(GetPtr())->Serialize(art, writer);
	case ARTNodeType::NODE_256:
		return art.n256_nodes->Get<Node256>(GetPtr())->Serialize(art, writer);
	default:
		throw InternalException("Invalid node type for Serialize.");
	}
}

void ARTNode::Deserialize(ART &art, idx_t block_id, idx_t offset) {

	MetaBlockReader reader(art.table_io_manager.GetIndexBlockManager(), block_id);
	reader.offset = offset;

	auto type_byte = reader.Read<uint8_t>();
	ARTNodeType type((ARTNodeType)(type_byte));

	switch (type) {
	case ARTNodeType::LEAF:
		SetPtr(art.leaves->New(), type);
		return art.leaves->Get<Leaf>(GetPtr())->Deserialize(art, reader);
	case ARTNodeType::NODE_4:
		SetPtr(art.n4_nodes->New(), type);
		return art.n4_nodes->Get<Node4>(GetPtr())->Deserialize(art, reader);
	case ARTNodeType::NODE_16:
		SetPtr(art.n16_nodes->New(), type);
		return art.n16_nodes->Get<Node16>(GetPtr())->Deserialize(art, reader);
	case ARTNodeType::NODE_48:
		SetPtr(art.n48_nodes->New(), type);
		return art.n48_nodes->Get<Node48>(GetPtr())->Deserialize(art, reader);
	case ARTNodeType::NODE_256:
		SetPtr(art.n256_nodes->New(), type);
		return art.n256_nodes->Get<Node256>(GetPtr())->Deserialize(art, reader);
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
		return art.leaves->Get<Leaf>(GetPtr())->ToString(art);
	}

	string str = "Node" + to_string(GetCapacity()) + ": [";

	auto next_pos = GetNextPosition(art, DConstants::INVALID_INDEX);
	while (next_pos != DConstants::INVALID_INDEX) {
		auto child = GetChild(art, next_pos);
		str += "(" + to_string(next_pos) + ", " + child->ToString(art) + ")";
		next_pos = GetNextPosition(art, next_pos);
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
		return &art.leaves->Get<Leaf>(GetPtr())->prefix;
	case ARTNodeType::NODE_4:
		return &art.n4_nodes->Get<Node4>(GetPtr())->prefix;
	case ARTNodeType::NODE_16:
		return &art.n16_nodes->Get<Node16>(GetPtr())->prefix;
	case ARTNodeType::NODE_48:
		return &art.n48_nodes->Get<Node48>(GetPtr())->prefix;
	case ARTNodeType::NODE_256:
		return &art.n256_nodes->Get<Node256>(GetPtr())->prefix;
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

void ARTNode::InitializeMerge(ART &art, const vector<idx_t> &buffer_counts) {

	if (!*this) {
		return;
	}

	if (IsSwizzled()) {
		auto block_info = GetBlockInfo();
		Deserialize(art, block_info.block_id, block_info.offset);
	}

	// if not all prefixes are inlined
	if (buffer_counts[(uint8_t)ARTNodeType::PREFIX_SEGMENT - 1] != 0) {
		// initialize prefix segments
		GetPrefix(art)->InitializeMerge(art, buffer_counts[(uint8_t)ARTNodeType::PREFIX_SEGMENT - 1]);
	}

	auto type = DecodeARTNodeType();
	switch (type) {
	case ARTNodeType::LEAF:
		// if not all leaves are inlined
		if (buffer_counts[(uint8_t)ARTNodeType::LEAF_SEGMENT - 1] != 0) {
			// initialize leaf segments
			art.leaves->Get<Leaf>(GetPtr())->InitializeMerge(art,
			                                                 buffer_counts[(uint8_t)ARTNodeType::LEAF_SEGMENT - 1]);
		}
		break;
	case ARTNodeType::NODE_4:
		art.n4_nodes->Get<Node4>(GetPtr())->InitializeMerge(art, buffer_counts);
		break;
	case ARTNodeType::NODE_16:
		art.n16_nodes->Get<Node16>(GetPtr())->InitializeMerge(art, buffer_counts);
		break;
	case ARTNodeType::NODE_48:
		art.n48_nodes->Get<Node48>(GetPtr())->InitializeMerge(art, buffer_counts);
		break;
	case ARTNodeType::NODE_256:
		art.n256_nodes->Get<Node256>(GetPtr())->InitializeMerge(art, buffer_counts);
		break;
	default:
		throw InternalException("Invalid node type for InitializeMerge.");
	}

	pointer += buffer_counts[(uint8_t)type - 1];
}

bool ARTNode::Merge(ART &art, ARTNode &other) {

	if (!*this) {
		*this = other;
		other = ARTNode();
		return true;
	}

	return ResolvePrefixes(art, other);
}

bool ARTNode::ResolvePrefixes(ART &art, ARTNode &other) {

	// NOTE: we always merge into the left ART

	D_ASSERT(*this);
	D_ASSERT(other);

	// make sure that r_node has the longer (or equally long) prefix
	if (GetPrefix(art)->count > other.GetPrefix(art)->count) {
		std::swap(*this, other);
	}

	ARTNode null_parent;
	auto &l_node = *this;
	auto &r_node = other;
	auto l_prefix = l_node.GetPrefix(art);
	auto r_prefix = r_node.GetPrefix(art);

	auto mismatch_position = l_prefix->MismatchPosition(art, *r_prefix);

	// both nodes have no prefix or the same prefix
	if (mismatch_position == l_prefix->count && l_prefix->count == r_prefix->count) {
		return MergeInternal(art, r_node);
	}

	if (mismatch_position == l_prefix->count) {
		// r_node's prefix contains l_node's prefix
		// l_node cannot be a leaf, otherwise the key represented by l_node would be a subset of another key
		// which is not possible by our construction
		D_ASSERT(l_node.DecodeARTNodeType() != ARTNodeType::LEAF);

		// test if the next byte (mismatch_position) in r_node (longer prefix) exists in l_node
		auto mismatch_byte = r_prefix->GetByte(art, mismatch_position);
		auto child_position = l_node.GetChildPosition(art, mismatch_byte);

		// update the prefix of r_node to only consist of the bytes after mismatch_position
		r_prefix->Reduce(art, mismatch_position);

		// insert r_node as a child of l_node at empty position
		if (child_position == DConstants::INVALID_INDEX) {

			ARTNode::InsertChild(art, l_node, mismatch_byte, r_node);
			r_node = ARTNode();
			return true;
		}

		// recurse
		auto child_node = l_node.GetChild(art, child_position);
		return child_node->ResolvePrefixes(art, r_node);
	}

	// prefixes differ, create new node and insert both nodes as children

	// create new node
	auto old_l_node = l_node;
	auto new_n4 = Node4::New(art, l_node);
	new_n4->prefix.Initialize(art, *l_prefix, mismatch_position);

	// insert old l_node, break up prefix of old l_node
	auto key_byte = l_prefix->Reduce(art, mismatch_position);
	Node4::InsertChild(art, l_node, key_byte, old_l_node);

	// insert r_node, break up prefix of r_node
	key_byte = r_prefix->Reduce(art, mismatch_position);
	Node4::InsertChild(art, l_node, key_byte, r_node);

	r_node = ARTNode();
	return true;
}

bool ARTNode::MergeInternal(ART &art, ARTNode &other) {

	D_ASSERT(*this);
	D_ASSERT(other);

	// always try to merge the smaller node into the bigger node
	// because maybe there is enough free space in the bigger node to fit the smaller one
	// without too much recursion
	if (this->DecodeARTNodeType() < other.DecodeARTNodeType()) {
		std::swap(*this, other);
	}

	ARTNode empty_node;
	auto &l_node = *this;
	auto &r_node = other;

	if (r_node.DecodeARTNodeType() == ARTNodeType::LEAF) {
		D_ASSERT(l_node.DecodeARTNodeType() == ARTNodeType::LEAF);

		if (art.IsUnique()) {
			return false;
		}

		art.leaves->Get<Leaf>(GetPtr())->Merge(art, r_node);
		return true;
	}

	uint8_t key_byte;
	idx_t r_child_position = DConstants::INVALID_INDEX;

	while (true) {
		r_child_position = r_node.GetNextPositionAndByte(art, r_child_position, key_byte);
		if (r_child_position == DConstants::INVALID_INDEX) {
			break;
		}
		auto r_child = r_node.GetChild(art, r_child_position);
		auto l_child_position = l_node.GetChildPosition(art, key_byte);

		if (l_child_position == DConstants::INVALID_INDEX) {
			// insert child at empty position
			ARTNode::InsertChild(art, l_node, key_byte, *r_child);
			r_node.ReplaceChild(art, r_child_position, empty_node);

		} else {
			// recurse
			auto l_child = l_node.GetChild(art, l_child_position);
			if (!l_child->ResolvePrefixes(art, *r_child)) {
				return false;
			}
		}
	}

	ARTNode::Free(art, r_node);
	return true;
}

//===--------------------------------------------------------------------===//
// Vacuum
//===--------------------------------------------------------------------===//

void ARTNode::Vacuum(ART &art, ARTNode &node, const vector<bool> &vacuum_nodes) {

	if (node.IsSwizzled()) {
		return;
	}

	// possibly vacuum prefix segments, if not all prefixes are inlined
	if (vacuum_nodes[(uint8_t)ARTNodeType::PREFIX_SEGMENT - 1] != 0) {
		// vacuum prefix segments
		node.GetPrefix(art)->Vacuum(art);
	}

	auto type = node.DecodeARTNodeType();
	auto needs_vacuum = vacuum_nodes[(uint8_t)type - 1];
	auto ptr = node.GetPtr();

	switch (type) {
	case ARTNodeType::LEAF: {
		if (needs_vacuum && art.leaves->NeedsVacuum(ptr)) {
			node.SetPtr(art.leaves->Vacuum(ptr), type);
		}
		// possibly vacuum leaf segments, if not all leaves are inlined
		if (vacuum_nodes[(uint8_t)ARTNodeType::LEAF_SEGMENT - 1] != 0) {
			art.leaves->Get<Leaf>(node.GetPtr())->Vacuum(art);
		}
		return;
	}
	case ARTNodeType::NODE_4:
		if (needs_vacuum && art.n4_nodes->NeedsVacuum(ptr)) {
			node.SetPtr(art.n4_nodes->Vacuum(ptr), type);
		}
		return art.n4_nodes->Get<Node4>(node.GetPtr())->Vacuum(art, vacuum_nodes);
	case ARTNodeType::NODE_16:
		if (needs_vacuum && art.n16_nodes->NeedsVacuum(ptr)) {
			node.SetPtr(art.n16_nodes->Vacuum(ptr), type);
		}
		return art.n16_nodes->Get<Node16>(node.GetPtr())->Vacuum(art, vacuum_nodes);
	case ARTNodeType::NODE_48:
		if (needs_vacuum && art.n48_nodes->NeedsVacuum(ptr)) {
			node.SetPtr(art.n48_nodes->Vacuum(ptr), type);
		}
		return art.n48_nodes->Get<Node48>(node.GetPtr())->Vacuum(art, vacuum_nodes);
	case ARTNodeType::NODE_256:
		if (needs_vacuum && art.n256_nodes->NeedsVacuum(ptr)) {
			node.SetPtr(art.n256_nodes->Vacuum(ptr), type);
		}
		return art.n256_nodes->Get<Node256>(node.GetPtr())->Vacuum(art, vacuum_nodes);
	default:
		throw InternalException("Invalid node type for Vacuum.");
	}
}

} // namespace duckdb
