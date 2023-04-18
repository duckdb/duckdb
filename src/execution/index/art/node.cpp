#include "duckdb/common/limits.hpp"
#include "duckdb/common/swap.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/leaf_segment.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/node256.hpp"
#include "duckdb/execution/index/art/node4.hpp"
#include "duckdb/execution/index/art/node48.hpp"
#include "duckdb/execution/index/art/prefix.hpp"
#include "duckdb/execution/index/art/prefix_segment.hpp"
#include "duckdb/storage/meta_block_reader.hpp"
#include "duckdb/storage/meta_block_writer.hpp"
#include "duckdb/storage/table_io_manager.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Constructors / Destructors
//===--------------------------------------------------------------------===//

Node::Node() : SwizzleablePointer() {
}

Node::Node(MetaBlockReader &reader) : SwizzleablePointer(reader) {
}

void Node::New(ART &art, Node &node, const NType type) {

	switch (type) {
	case NType::PREFIX_SEGMENT:
		PrefixSegment::New(art, node);
		break;
	case NType::LEAF_SEGMENT:
		LeafSegment::New(art, node);
		break;
	case NType::NODE_4:
		Node4::New(art, node);
		break;
	case NType::NODE_16:
		Node16::New(art, node);
		break;
	case NType::NODE_48:
		Node48::New(art, node);
		break;
	case NType::NODE_256:
		Node256::New(art, node);
		break;
	default:
		throw InternalException("Invalid node type for New.");
	}
}

void Node::Free(ART &art, Node &node) {

	// recursively free all nodes that are in-memory, and skip swizzled and empty nodes

	if (!node.IsSet()) {
		return;
	}

	if (!node.IsSwizzled()) {

		auto type = node.DecodeARTNodeType();
		if (type != NType::PREFIX_SEGMENT && type != NType::LEAF_SEGMENT) {
			node.GetPrefix(art).Free(art);
		}

		// free the prefixes and children of the nodes
		switch (type) {
		case NType::LEAF:
			Leaf::Free(art, node);
			break;
		case NType::NODE_4:
			Node4::Free(art, node);
			break;
		case NType::NODE_16:
			Node16::Free(art, node);
			break;
		case NType::NODE_48:
			Node48::Free(art, node);
			break;
		case NType::NODE_256:
			Node256::Free(art, node);
			break;
		default:
			break;
		}

		Node::GetAllocator(art, type).Free(node);
	}

	// overwrite with an empty ART node
	node.Reset();
}

//===--------------------------------------------------------------------===//
// Inserts
//===--------------------------------------------------------------------===//

void Node::ReplaceChild(const ART &art, const uint8_t byte, const Node child) {

	D_ASSERT(!IsSwizzled());

	switch (DecodeARTNodeType()) {
	case NType::NODE_4:
		return Node4::Get(art, *this).ReplaceChild(byte, child);
	case NType::NODE_16:
		return Node16::Get(art, *this).ReplaceChild(byte, child);
	case NType::NODE_48:
		return Node48::Get(art, *this).ReplaceChild(byte, child);
	case NType::NODE_256:
		return Node256::Get(art, *this).ReplaceChild(byte, child);
	default:
		throw InternalException("Invalid node type for ReplaceChild.");
	}
}

void Node::InsertChild(ART &art, Node &node, const uint8_t byte, const Node child) {

	switch (node.DecodeARTNodeType()) {
	case NType::NODE_4:
		return Node4::InsertChild(art, node, byte, child);
	case NType::NODE_16:
		return Node16::InsertChild(art, node, byte, child);
	case NType::NODE_48:
		return Node48::InsertChild(art, node, byte, child);
	case NType::NODE_256:
		return Node256::InsertChild(art, node, byte, child);
	default:
		throw InternalException("Invalid node type for InsertChild.");
	}
}

//===--------------------------------------------------------------------===//
// Deletes
//===--------------------------------------------------------------------===//

void Node::DeleteChild(ART &art, Node &node, const uint8_t byte) {

	switch (node.DecodeARTNodeType()) {
	case NType::NODE_4:
		return Node4::DeleteChild(art, node, byte);
	case NType::NODE_16:
		return Node16::DeleteChild(art, node, byte);
	case NType::NODE_48:
		return Node48::DeleteChild(art, node, byte);
	case NType::NODE_256:
		return Node256::DeleteChild(art, node, byte);
	default:
		throw InternalException("Invalid node type for DeleteChild.");
	}
}

//===--------------------------------------------------------------------===//
// Get functions
//===--------------------------------------------------------------------===//

optional_ptr<Node> Node::GetChild(ART &art, const uint8_t byte) const {

	D_ASSERT(!IsSwizzled());

	optional_ptr<Node> child;
	switch (DecodeARTNodeType()) {
	case NType::NODE_4: {
		child = Node4::Get(art, *this).GetChild(byte);
		break;
	}
	case NType::NODE_16: {
		child = Node16::Get(art, *this).GetChild(byte);
		break;
	}
	case NType::NODE_48: {
		child = Node48::Get(art, *this).GetChild(byte);
		break;
	}
	case NType::NODE_256: {
		child = Node256::Get(art, *this).GetChild(byte);
		break;
	}
	default:
		throw InternalException("Invalid node type for GetChild.");
	}

	// unswizzle the ART node before returning it
	if (child && child->IsSwizzled()) {
		child->Deserialize(art);
	}
	return child;
}

optional_ptr<Node> Node::GetNextChild(ART &art, uint8_t &byte) const {

	D_ASSERT(!IsSwizzled());

	optional_ptr<Node> child;
	switch (DecodeARTNodeType()) {
	case NType::NODE_4: {
		child = Node4::Get(art, *this).GetNextChild(byte);
		break;
	}
	case NType::NODE_16: {
		child = Node16::Get(art, *this).GetNextChild(byte);
		break;
	}
	case NType::NODE_48: {
		child = Node48::Get(art, *this).GetNextChild(byte);
		break;
	}
	case NType::NODE_256: {
		child = Node256::Get(art, *this).GetNextChild(byte);
		break;
	}
	default:
		throw InternalException("Invalid node type for GetNextChild.");
	}

	// unswizzle the ART node before returning it
	if (child && child->IsSwizzled()) {
		child->Deserialize(art);
	}
	return child;
}

//===--------------------------------------------------------------------===//
// (De)serialization
//===--------------------------------------------------------------------===//

BlockPointer Node::Serialize(ART &art, MetaBlockWriter &writer) {

	if (!IsSet()) {
		return {(block_id_t)DConstants::INVALID_INDEX, 0};
	}

	if (IsSwizzled()) {
		Deserialize(art);
	}

	switch (DecodeARTNodeType()) {
	case NType::LEAF:
		return Leaf::Get(art, *this).Serialize(art, writer);
	case NType::NODE_4:
		return Node4::Get(art, *this).Serialize(art, writer);
	case NType::NODE_16:
		return Node16::Get(art, *this).Serialize(art, writer);
	case NType::NODE_48:
		return Node48::Get(art, *this).Serialize(art, writer);
	case NType::NODE_256:
		return Node256::Get(art, *this).Serialize(art, writer);
	default:
		throw InternalException("Invalid node type for Serialize.");
	}
}

void Node::Deserialize(ART &art) {

	MetaBlockReader reader(art.table_io_manager.GetIndexBlockManager(), buffer_id);
	reader.offset = offset;
	type = reader.Read<uint8_t>();
	swizzle_flag = 0;

	auto type = DecodeARTNodeType();
	SetPtr(Node::GetAllocator(art, type).New());

	switch (type) {
	case NType::LEAF:
		return Leaf::Get(art, *this).Deserialize(art, reader);
	case NType::NODE_4:
		return Node4::Get(art, *this).Deserialize(art, reader);
	case NType::NODE_16:
		return Node16::Get(art, *this).Deserialize(art, reader);
	case NType::NODE_48:
		return Node48::Get(art, *this).Deserialize(art, reader);
	case NType::NODE_256:
		return Node256::Get(art, *this).Deserialize(art, reader);
	default:
		throw InternalException("Invalid node type for Deserialize.");
	}
}

//===--------------------------------------------------------------------===//
// Utility
//===--------------------------------------------------------------------===//

string Node::ToString(ART &art) const {

	D_ASSERT(!IsSwizzled());

	if (DecodeARTNodeType() == NType::LEAF) {
		return Leaf::Get(art, *this).ToString(art);
	}

	string str = "Node" + to_string(GetCapacity()) + ": [";

	uint8_t byte = 0;
	auto child = GetNextChild(art, byte);
	while (child) {
		str += "(" + to_string(byte) + ", " + child->ToString(art) + ")";
		if (byte == NumericLimits<uint8_t>::Maximum()) {
			break;
		}
		byte++;
		child = GetNextChild(art, byte);
	}

	return str + "]";
}

idx_t Node::GetCapacity() const {

	D_ASSERT(!IsSwizzled());

	switch (DecodeARTNodeType()) {
	case NType::NODE_4:
		return Node::NODE_4_CAPACITY;
	case NType::NODE_16:
		return Node::NODE_16_CAPACITY;
	case NType::NODE_48:
		return Node::NODE_48_CAPACITY;
	case NType::NODE_256:
		return Node::NODE_256_CAPACITY;
	default:
		throw InternalException("Invalid node type for GetCapacity.");
	}
}

Prefix &Node::GetPrefix(ART &art) {

	if (IsSwizzled()) {
		Deserialize(art);
	}

	switch (DecodeARTNodeType()) {
	case NType::LEAF:
		return Leaf::Get(art, *this).prefix;
	case NType::NODE_4:
		return Node4::Get(art, *this).prefix;
	case NType::NODE_16:
		return Node16::Get(art, *this).prefix;
	case NType::NODE_48:
		return Node48::Get(art, *this).prefix;
	case NType::NODE_256:
		return Node256::Get(art, *this).prefix;
	default:
		throw InternalException("Invalid node type for GetPrefix.");
	}
}

NType Node::GetARTNodeTypeByCount(const idx_t count) {

	if (count <= NODE_4_CAPACITY) {
		return NType::NODE_4;
	} else if (count <= NODE_16_CAPACITY) {
		return NType::NODE_16;
	} else if (count <= NODE_48_CAPACITY) {
		return NType::NODE_48;
	}
	return NType::NODE_256;
}

FixedSizeAllocator &Node::GetAllocator(const ART &art, NType type) {
	return *art.allocators[(uint8_t)type - 1];
}

//===--------------------------------------------------------------------===//
// Merging
//===--------------------------------------------------------------------===//

void Node::InitializeMerge(ART &art, const ARTFlags &flags) {

	if (!IsSet()) {
		return;
	}

	if (IsSwizzled()) {
		Deserialize(art);
	}

	// if not all prefixes are inlined
	if (flags.merge_buffer_counts[(uint8_t)NType::PREFIX_SEGMENT - 1] != 0) {
		// initialize prefix segments
		GetPrefix(art).InitializeMerge(art, flags.merge_buffer_counts[(uint8_t)NType::PREFIX_SEGMENT - 1]);
	}

	auto type = DecodeARTNodeType();
	switch (type) {
	case NType::LEAF:
		// if not all leaves are inlined
		if (flags.merge_buffer_counts[(uint8_t)NType::LEAF_SEGMENT - 1] != 0) {
			// initialize leaf segments
			Leaf::Get(art, *this).InitializeMerge(art, flags.merge_buffer_counts[(uint8_t)NType::LEAF_SEGMENT - 1]);
		}
		break;
	case NType::NODE_4:
		Node4::Get(art, *this).InitializeMerge(art, flags);
		break;
	case NType::NODE_16:
		Node16::Get(art, *this).InitializeMerge(art, flags);
		break;
	case NType::NODE_48:
		Node48::Get(art, *this).InitializeMerge(art, flags);
		break;
	case NType::NODE_256:
		Node256::Get(art, *this).InitializeMerge(art, flags);
		break;
	default:
		throw InternalException("Invalid node type for InitializeMerge.");
	}

	buffer_id += flags.merge_buffer_counts[(uint8_t)type - 1];
}

bool Node::Merge(ART &art, Node &other) {

	if (!IsSet()) {
		*this = other;
		other = Node();
		return true;
	}

	return ResolvePrefixes(art, other);
}

bool Node::ResolvePrefixes(ART &art, Node &other) {

	// NOTE: we always merge into the left ART

	D_ASSERT(IsSet());
	D_ASSERT(other.IsSet());

	// make sure that r_node has the longer (or equally long) prefix
	if (GetPrefix(art).count > other.GetPrefix(art).count) {
		swap(*this, other);
	}

	auto &l_node = *this;
	auto &r_node = other;
	auto &l_prefix = l_node.GetPrefix(art);
	auto &r_prefix = r_node.GetPrefix(art);

	auto mismatch_position = l_prefix.MismatchPosition(art, r_prefix);

	// both nodes have no prefix or the same prefix
	if (mismatch_position == l_prefix.count && l_prefix.count == r_prefix.count) {
		return MergeInternal(art, r_node);
	}

	if (mismatch_position == l_prefix.count) {
		// r_node's prefix contains l_node's prefix
		// l_node cannot be a leaf, otherwise the key represented by l_node would be a subset of another key
		// which is not possible by our construction
		D_ASSERT(l_node.DecodeARTNodeType() != NType::LEAF);

		// test if the next byte (mismatch_position) in r_node (longer prefix) exists in l_node
		auto mismatch_byte = r_prefix.GetByte(art, mismatch_position);
		auto child_node = l_node.GetChild(art, mismatch_byte);

		// update the prefix of r_node to only consist of the bytes after mismatch_position
		r_prefix.Reduce(art, mismatch_position);

		// insert r_node as a child of l_node at empty position
		if (!child_node) {
			Node::InsertChild(art, l_node, mismatch_byte, r_node);
			r_node.Reset();
			return true;
		}

		// recurse
		return child_node->ResolvePrefixes(art, r_node);
	}

	// prefixes differ, create new node and insert both nodes as children

	// create new node
	auto old_l_node = l_node;
	auto &new_n4 = Node4::New(art, l_node);
	new_n4.prefix.Initialize(art, l_prefix, mismatch_position);

	// insert old l_node, break up prefix of old l_node
	auto key_byte = l_prefix.Reduce(art, mismatch_position);
	Node4::InsertChild(art, l_node, key_byte, old_l_node);

	// insert r_node, break up prefix of r_node
	key_byte = r_prefix.Reduce(art, mismatch_position);
	Node4::InsertChild(art, l_node, key_byte, r_node);

	r_node.Reset();
	return true;
}

bool Node::MergeInternal(ART &art, Node &other) {

	D_ASSERT(IsSet());
	D_ASSERT(other.IsSet());

	// always try to merge the smaller node into the bigger node
	// because maybe there is enough free space in the bigger node to fit the smaller one
	// without too much recursion
	if (this->DecodeARTNodeType() < other.DecodeARTNodeType()) {
		swap(*this, other);
	}

	Node empty_node;
	auto &l_node = *this;
	auto &r_node = other;

	if (r_node.DecodeARTNodeType() == NType::LEAF) {
		D_ASSERT(l_node.DecodeARTNodeType() == NType::LEAF);

		if (art.IsUnique()) {
			return false;
		}

		Leaf::Get(art, *this).Merge(art, r_node);
		return true;
	}

	uint8_t byte = 0;
	auto r_child = r_node.GetNextChild(art, byte);

	// while r_node still has children to merge
	while (r_child) {
		auto l_child = l_node.GetChild(art, byte);
		if (!l_child) {
			// insert child at empty byte
			Node::InsertChild(art, l_node, byte, *r_child);
			r_node.ReplaceChild(art, byte, empty_node);

		} else {
			// recurse
			if (!l_child->ResolvePrefixes(art, *r_child)) {
				return false;
			}
		}

		if (byte == NumericLimits<uint8_t>::Maximum()) {
			break;
		}
		byte++;
		r_child = r_node.GetNextChild(art, byte);
	}

	Node::Free(art, r_node);
	return true;
}

//===--------------------------------------------------------------------===//
// Vacuum
//===--------------------------------------------------------------------===//

void Node::Vacuum(ART &art, Node &node, const ARTFlags &flags) {

	if (node.IsSwizzled()) {
		return;
	}

	// possibly vacuum prefix segments, if not all prefixes are inlined
	bool needs_vacuum = flags.vacuum_flags[(uint8_t)NType::PREFIX_SEGMENT - 1];
	if (needs_vacuum) {
		// vacuum prefix segments
		node.GetPrefix(art).Vacuum(art);
	}

	auto type = node.DecodeARTNodeType();
	auto &allocator = Node::GetAllocator(art, type);
	needs_vacuum = flags.vacuum_flags[node.type - 1] && allocator.NeedsVacuum(node);
	if (needs_vacuum) {
		node.SetPtr(allocator.VacuumPointer(node));
	}

	switch (type) {
	case NType::LEAF: {
		// possibly vacuum leaf segments, if not all leaves are inlined
		if (flags.vacuum_flags[(uint8_t)NType::LEAF_SEGMENT - 1]) {
			Leaf::Get(art, node).Vacuum(art);
		}
		return;
	}
	case NType::NODE_4:
		return Node4::Get(art, node).Vacuum(art, flags);
	case NType::NODE_16:
		return Node16::Get(art, node).Vacuum(art, flags);
	case NType::NODE_48:
		return Node48::Get(art, node).Vacuum(art, flags);
	case NType::NODE_256:
		return Node256::Get(art, node).Vacuum(art, flags);
	default:
		throw InternalException("Invalid node type for Vacuum.");
	}
}

} // namespace duckdb
