#include "duckdb/execution/index/art/node.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/swap.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node256.hpp"
#include "duckdb/execution/index/art/node48.hpp"
#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/node4.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/leaf_segment.hpp"
#include "duckdb/execution/index/art/prefix.hpp"
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

	// NOTE: leaves and prefixes should not pass through this function

	switch (type) {
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

		// free the children of the nodes
		auto type = node.DecodeARTNodeType();
		switch (type) {
		case NType::PREFIX:
			Prefix::Free(art, node);
			break;
		case NType::LEAF_SEGMENT:
			LeafSegment::Free(art, node);
			break;
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

void Node::DeleteChild(ART &art, Node &node, Node &prefix, const uint8_t byte) {

	switch (node.DecodeARTNodeType()) {
	case NType::NODE_4:
		return Node4::DeleteChild(art, node, prefix, byte);
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

	D_ASSERT(IsSet() && !IsSwizzled());

	optional_ptr<Node> child;
	switch (DecodeARTNodeType()) {
	case NType::NODE_4:
		child = Node4::Get(art, *this).GetChild(byte);
		break;
	case NType::NODE_16:
		child = Node16::Get(art, *this).GetChild(byte);
		break;
	case NType::NODE_48:
		child = Node48::Get(art, *this).GetChild(byte);
		break;
	case NType::NODE_256:
		child = Node256::Get(art, *this).GetChild(byte);
		break;
	default:
		throw InternalException("Invalid node type for GetChild.");
	}

	// deserialize the ART node before returning it
	if (child && child->IsSwizzled()) {
		child->Deserialize(art);
	}
	return child;
}

optional_ptr<Node> Node::GetNextChild(ART &art, uint8_t &byte, const bool deserialize) const {

	D_ASSERT(IsSet() && !IsSwizzled());

	optional_ptr<Node> child;
	switch (DecodeARTNodeType()) {
	case NType::NODE_4:
		child = Node4::Get(art, *this).GetNextChild(byte);
		break;
	case NType::NODE_16:
		child = Node16::Get(art, *this).GetNextChild(byte);
		break;
	case NType::NODE_48:
		child = Node48::Get(art, *this).GetNextChild(byte);
		break;
	case NType::NODE_256:
		child = Node256::Get(art, *this).GetNextChild(byte);
		break;
	default:
		throw InternalException("Invalid node type for GetNextChild.");
	}

	// deserialize the ART node before returning it
	if (child && deserialize && child->IsSwizzled()) {
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
	case NType::PREFIX:
		return Prefix::Get(art, *this).Serialize(art, writer);
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

	auto decoded_type = DecodeARTNodeType();
	SetPtr(Node::GetAllocator(art, decoded_type).New());
	type = (uint8_t)decoded_type;

	switch (decoded_type) {
	case NType::PREFIX:
		return Prefix::Get(art, *this).Deserialize(reader);
	case NType::LEAF:
		return Leaf::Get(art, *this).Deserialize(art, reader);
	case NType::NODE_4:
		return Node4::Get(art, *this).Deserialize(reader);
	case NType::NODE_16:
		return Node16::Get(art, *this).Deserialize(reader);
	case NType::NODE_48:
		return Node48::Get(art, *this).Deserialize(reader);
	case NType::NODE_256:
		return Node256::Get(art, *this).Deserialize(reader);
	default:
		throw InternalException("Invalid node type for Deserialize.");
	}
}

//===--------------------------------------------------------------------===//
// Utility
//===--------------------------------------------------------------------===//

string Node::VerifyAndToString(ART &art, const bool only_verify) {

	D_ASSERT(IsSet());
	if (IsSwizzled()) {
		return only_verify ? "" : "swizzled";
	}

	auto type = DecodeARTNodeType();
	if (type == NType::LEAF) {
		auto str = Leaf::Get(art, *this).VerifyAndToString(art, only_verify);
		return only_verify ? "" : "\n" + str;
	}
	if (type == NType::PREFIX) {
		auto str = Prefix::Get(art, *this).VerifyAndToString(art, only_verify);
		return only_verify ? "" : "\n" + str;
	}

	string str = "Node" + to_string(GetCapacity()) + ": [";

	idx_t child_count = 0;
	uint8_t byte = 0;
	auto child = GetNextChild(art, byte, false);

	while (child) {

		child_count++;
		if (child->IsSwizzled()) {
			if (!only_verify) {
				str += "(swizzled)";
			}
		} else {
			str += "(" + to_string(byte) + ", " + child->VerifyAndToString(art, only_verify) + ")";
			if (byte == NumericLimits<uint8_t>::Maximum()) {
				break;
			}
		}

		byte++;
		child = GetNextChild(art, byte, false);
	}

	(void)child_count;
	// ensure that the child count is at least two
	D_ASSERT(child_count > 1);
	return only_verify ? "" : "\n" + str + "]";
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

	// the index is fully in memory during CREATE [UNIQUE] INDEX statements
	D_ASSERT(IsSet() && !IsSwizzled());

	auto type = DecodeARTNodeType();
	switch (type) {
	case NType::PREFIX:
		Prefix::Get(art, *this).InitializeMerge(art, flags);
		break;
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

bool MergePrefixContainsOtherPrefix(ART &art, reference<Node> &l_node, reference<Node> &r_node,
                                    idx_t &mismatch_position) {

	// r_node's prefix contains l_node's prefix
	// l_node cannot be a leaf, otherwise the key represented by l_node would be a subset of another key
	// which is not possible by our construction
	D_ASSERT(l_node.get().DecodeARTNodeType() != NType::LEAF);

	// test if the next byte (mismatch_position) in r_node (prefix) exists in l_node
	auto mismatch_byte = Prefix::GetByte(art, r_node, mismatch_position);
	auto child_node = l_node.get().GetChild(art, mismatch_byte);

	// update the prefix of r_node to only consist of the bytes after mismatch_position
	Prefix::Reduce(art, r_node, mismatch_position);

	if (!child_node) {
		// insert r_node as a child of l_node at the empty position
		Node::InsertChild(art, l_node, mismatch_byte, r_node);
		r_node.get().Reset();
		return true;
	}

	// recurse
	return child_node->ResolvePrefixes(art, r_node);
}

void MergePrefixesDiffer(ART &art, reference<Node> &l_node, reference<Node> &r_node, idx_t &mismatch_position) {

	// create a new node and insert both nodes as children

	Node l_child;
	auto l_byte = Prefix::GetByte(art, l_node, mismatch_position);
	Prefix::Split(art, l_node, l_child, mismatch_position);
	Node4::New(art, l_node);

	// insert children
	Node4::InsertChild(art, l_node, l_byte, l_child);
	auto r_byte = Prefix::GetByte(art, r_node, mismatch_position);
	Prefix::Reduce(art, r_node, mismatch_position);
	Node4::InsertChild(art, l_node, r_byte, r_node);

	r_node.get().Reset();
}

bool Node::ResolvePrefixes(ART &art, Node &other) {

	// NOTE: we always merge into the left ART

	D_ASSERT(IsSet());
	D_ASSERT(other.IsSet());

	// case 1: both nodes have no prefix
	if (DecodeARTNodeType() != NType::PREFIX && other.DecodeARTNodeType() != NType::PREFIX) {
		return MergeInternal(art, other);
	}

	reference<Node> l_node(*this);
	reference<Node> r_node(other);

	idx_t mismatch_position = DConstants::INVALID_INDEX;

	// traverse prefixes
	if (l_node.get().DecodeARTNodeType() == NType::PREFIX && r_node.get().DecodeARTNodeType() == NType::PREFIX) {

		if (!Prefix::Traverse(art, l_node, r_node, mismatch_position)) {
			return false;
		}
		// we already recurse because the prefixes matched (so far)
		if (mismatch_position == DConstants::INVALID_INDEX) {
			return true;
		}

	} else {

		// l_prefix contains r_prefix
		if (l_node.get().DecodeARTNodeType() == NType::PREFIX) {
			swap(*this, other);
		}
		mismatch_position = 0;
	}
	D_ASSERT(mismatch_position != DConstants::INVALID_INDEX);

	// case 2: one prefix contains the other prefix
	if (l_node.get().DecodeARTNodeType() != NType::PREFIX && r_node.get().DecodeARTNodeType() == NType::PREFIX) {
		return MergePrefixContainsOtherPrefix(art, l_node, r_node, mismatch_position);
	}

	// case 3: prefixes differ at a specific byte
	MergePrefixesDiffer(art, l_node, r_node, mismatch_position);
	return true;
}

bool Node::MergeInternal(ART &art, Node &other) {

	D_ASSERT(IsSet() && other.IsSet());
	D_ASSERT(DecodeARTNodeType() != NType::PREFIX && DecodeARTNodeType() != NType::LEAF_SEGMENT);
	D_ASSERT(other.DecodeARTNodeType() != NType::PREFIX && other.DecodeARTNodeType() != NType::LEAF_SEGMENT);

	// always try to merge the smaller node into the bigger node
	// because maybe there is enough free space in the bigger node to fit the smaller one
	// without too much recursion
	if (DecodeARTNodeType() < other.DecodeARTNodeType()) {
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

		Leaf::Get(art, l_node).Merge(art, r_node);
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

	auto type = node.DecodeARTNodeType();
	auto &allocator = Node::GetAllocator(art, type);
	auto needs_vacuum = flags.vacuum_flags[node.type - 1] && allocator.NeedsVacuum(node);
	if (needs_vacuum) {
		node.SetPtr(allocator.VacuumPointer(node));
		node.type = (uint8_t)type;
	}

	switch (type) {
	case NType::PREFIX:
		return Prefix::Get(art, node).Vacuum(art, flags);
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
