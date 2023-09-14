#include "duckdb/execution/index/art/node.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/swap.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node256.hpp"
#include "duckdb/execution/index/art/node48.hpp"
#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/node4.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/prefix.hpp"
#include "duckdb/storage/table_io_manager.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// New / Free
//===--------------------------------------------------------------------===//

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

	if (!node.HasMetadata()) {
		return node.Clear();
	}

	// free the children of the nodes
	auto type = node.GetType();
	switch (type) {
	case NType::PREFIX:
		// iterative
		return Prefix::Free(art, node);
	case NType::LEAF:
		// iterative
		return Leaf::Free(art, node);
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
	case NType::LEAF_INLINED:
		return node.Clear();
	}

	GetAllocator(art, type).Free(node);
	node.Clear();
}

//===--------------------------------------------------------------------===//
// Get Allocators
//===--------------------------------------------------------------------===//

FixedSizeAllocator &Node::GetAllocator(const ART &art, const NType type) {
	return *(*art.allocators)[static_cast<uint8_t>(type) - 1];
}

//===--------------------------------------------------------------------===//
// Inserts
//===--------------------------------------------------------------------===//

void Node::ReplaceChild(const ART &art, const uint8_t byte, const Node child) const {

	switch (GetType()) {
	case NType::NODE_4:
		return RefMutable<Node4>(art, *this, NType::NODE_4).ReplaceChild(byte, child);
	case NType::NODE_16:
		return RefMutable<Node16>(art, *this, NType::NODE_16).ReplaceChild(byte, child);
	case NType::NODE_48:
		return RefMutable<Node48>(art, *this, NType::NODE_48).ReplaceChild(byte, child);
	case NType::NODE_256:
		return RefMutable<Node256>(art, *this, NType::NODE_256).ReplaceChild(byte, child);
	default:
		throw InternalException("Invalid node type for ReplaceChild.");
	}
}

void Node::InsertChild(ART &art, Node &node, const uint8_t byte, const Node child) {

	switch (node.GetType()) {
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

	switch (node.GetType()) {
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

optional_ptr<const Node> Node::GetChild(ART &art, const uint8_t byte) const {

	D_ASSERT(HasMetadata());

	switch (GetType()) {
	case NType::NODE_4:
		return Ref<const Node4>(art, *this, NType::NODE_4).GetChild(byte);
	case NType::NODE_16:
		return Ref<const Node16>(art, *this, NType::NODE_16).GetChild(byte);
	case NType::NODE_48:
		return Ref<const Node48>(art, *this, NType::NODE_48).GetChild(byte);
	case NType::NODE_256:
		return Ref<const Node256>(art, *this, NType::NODE_256).GetChild(byte);
	default:
		throw InternalException("Invalid node type for GetChild.");
	}
}

optional_ptr<Node> Node::GetChildMutable(ART &art, const uint8_t byte) const {

	D_ASSERT(HasMetadata());

	switch (GetType()) {
	case NType::NODE_4:
		return RefMutable<Node4>(art, *this, NType::NODE_4).GetChildMutable(byte);
	case NType::NODE_16:
		return RefMutable<Node16>(art, *this, NType::NODE_16).GetChildMutable(byte);
	case NType::NODE_48:
		return RefMutable<Node48>(art, *this, NType::NODE_48).GetChildMutable(byte);
	case NType::NODE_256:
		return RefMutable<Node256>(art, *this, NType::NODE_256).GetChildMutable(byte);
	default:
		throw InternalException("Invalid node type for GetChildMutable.");
	}
}

optional_ptr<const Node> Node::GetNextChild(ART &art, uint8_t &byte) const {

	D_ASSERT(HasMetadata());

	switch (GetType()) {
	case NType::NODE_4:
		return Ref<const Node4>(art, *this, NType::NODE_4).GetNextChild(byte);
	case NType::NODE_16:
		return Ref<const Node16>(art, *this, NType::NODE_16).GetNextChild(byte);
	case NType::NODE_48:
		return Ref<const Node48>(art, *this, NType::NODE_48).GetNextChild(byte);
	case NType::NODE_256:
		return Ref<const Node256>(art, *this, NType::NODE_256).GetNextChild(byte);
	default:
		throw InternalException("Invalid node type for GetNextChild.");
	}
}

optional_ptr<Node> Node::GetNextChildMutable(ART &art, uint8_t &byte) const {

	D_ASSERT(HasMetadata());

	switch (GetType()) {
	case NType::NODE_4:
		return RefMutable<Node4>(art, *this, NType::NODE_4).GetNextChildMutable(byte);
	case NType::NODE_16:
		return RefMutable<Node16>(art, *this, NType::NODE_16).GetNextChildMutable(byte);
	case NType::NODE_48:
		return RefMutable<Node48>(art, *this, NType::NODE_48).GetNextChildMutable(byte);
	case NType::NODE_256:
		return RefMutable<Node256>(art, *this, NType::NODE_256).GetNextChildMutable(byte);
	default:
		throw InternalException("Invalid node type for GetNextChildMutable.");
	}
}

//===--------------------------------------------------------------------===//
// Utility
//===--------------------------------------------------------------------===//

string Node::VerifyAndToString(ART &art, const bool only_verify) const {

	D_ASSERT(HasMetadata());

	if (GetType() == NType::LEAF || GetType() == NType::LEAF_INLINED) {
		auto str = Leaf::VerifyAndToString(art, *this, only_verify);
		return only_verify ? "" : "\n" + str;
	}
	if (GetType() == NType::PREFIX) {
		auto str = Prefix::VerifyAndToString(art, *this, only_verify);
		return only_verify ? "" : "\n" + str;
	}

	string str = "Node" + to_string(GetCapacity()) + ": [";
	uint8_t byte = 0;
	auto child = GetNextChild(art, byte);

	while (child) {
		str += "(" + to_string(byte) + ", " + child->VerifyAndToString(art, only_verify) + ")";
		if (byte == NumericLimits<uint8_t>::Maximum()) {
			break;
		}

		byte++;
		child = GetNextChild(art, byte);
	}

	return only_verify ? "" : "\n" + str + "]";
}

idx_t Node::GetCapacity() const {

	switch (GetType()) {
	case NType::NODE_4:
		return NODE_4_CAPACITY;
	case NType::NODE_16:
		return NODE_16_CAPACITY;
	case NType::NODE_48:
		return NODE_48_CAPACITY;
	case NType::NODE_256:
		return NODE_256_CAPACITY;
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

//===--------------------------------------------------------------------===//
// Merging
//===--------------------------------------------------------------------===//

void Node::InitializeMerge(ART &art, const ARTFlags &flags) {

	D_ASSERT(HasMetadata());

	switch (GetType()) {
	case NType::PREFIX:
		// iterative
		return Prefix::InitializeMerge(art, *this, flags);
	case NType::LEAF:
		// iterative
		return Leaf::InitializeMerge(art, *this, flags);
	case NType::NODE_4:
		RefMutable<Node4>(art, *this, NType::NODE_4).InitializeMerge(art, flags);
		break;
	case NType::NODE_16:
		RefMutable<Node16>(art, *this, NType::NODE_16).InitializeMerge(art, flags);
		break;
	case NType::NODE_48:
		RefMutable<Node48>(art, *this, NType::NODE_48).InitializeMerge(art, flags);
		break;
	case NType::NODE_256:
		RefMutable<Node256>(art, *this, NType::NODE_256).InitializeMerge(art, flags);
		break;
	case NType::LEAF_INLINED:
		return;
	}

	IncreaseBufferId(flags.merge_buffer_counts[static_cast<uint8_t>(GetType()) - 1]);
}

bool Node::Merge(ART &art, Node &other) {

	if (!HasMetadata()) {
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
	D_ASSERT(l_node.get().GetType() != NType::LEAF && l_node.get().GetType() != NType::LEAF_INLINED);

	// test if the next byte (mismatch_position) in r_node (prefix) exists in l_node
	auto mismatch_byte = Prefix::GetByte(art, r_node, mismatch_position);
	auto child_node = l_node.get().GetChildMutable(art, mismatch_byte);

	// update the prefix of r_node to only consist of the bytes after mismatch_position
	Prefix::Reduce(art, r_node, mismatch_position);

	if (!child_node) {
		// insert r_node as a child of l_node at the empty position
		Node::InsertChild(art, l_node, mismatch_byte, r_node);
		r_node.get().Clear();
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

	r_node.get().Clear();
}

bool Node::ResolvePrefixes(ART &art, Node &other) {

	// NOTE: we always merge into the left ART

	D_ASSERT(HasMetadata() && other.HasMetadata());

	// case 1: both nodes have no prefix
	if (GetType() != NType::PREFIX && other.GetType() != NType::PREFIX) {
		return MergeInternal(art, other);
	}

	reference<Node> l_node(*this);
	reference<Node> r_node(other);

	idx_t mismatch_position = DConstants::INVALID_INDEX;

	// traverse prefixes
	if (l_node.get().GetType() == NType::PREFIX && r_node.get().GetType() == NType::PREFIX) {

		if (!Prefix::Traverse(art, l_node, r_node, mismatch_position)) {
			return false;
		}
		// we already recurse because the prefixes matched (so far)
		if (mismatch_position == DConstants::INVALID_INDEX) {
			return true;
		}

	} else {

		// l_prefix contains r_prefix
		if (l_node.get().GetType() == NType::PREFIX) {
			swap(*this, other);
		}
		mismatch_position = 0;
	}
	D_ASSERT(mismatch_position != DConstants::INVALID_INDEX);

	// case 2: one prefix contains the other prefix
	if (l_node.get().GetType() != NType::PREFIX && r_node.get().GetType() == NType::PREFIX) {
		return MergePrefixContainsOtherPrefix(art, l_node, r_node, mismatch_position);
	}

	// case 3: prefixes differ at a specific byte
	MergePrefixesDiffer(art, l_node, r_node, mismatch_position);
	return true;
}

bool Node::MergeInternal(ART &art, Node &other) {

	D_ASSERT(HasMetadata() && other.HasMetadata());
	D_ASSERT(GetType() != NType::PREFIX && other.GetType() != NType::PREFIX);

	// always try to merge the smaller node into the bigger node
	// because maybe there is enough free space in the bigger node to fit the smaller one
	// without too much recursion
	if (GetType() < other.GetType()) {
		swap(*this, other);
	}

	Node empty_node;
	auto &l_node = *this;
	auto &r_node = other;

	if (r_node.GetType() == NType::LEAF || r_node.GetType() == NType::LEAF_INLINED) {
		D_ASSERT(l_node.GetType() == NType::LEAF || l_node.GetType() == NType::LEAF_INLINED);

		if (art.IsUnique()) {
			return false;
		}

		Leaf::Merge(art, l_node, r_node);
		return true;
	}

	uint8_t byte = 0;
	auto r_child = r_node.GetNextChildMutable(art, byte);

	// while r_node still has children to merge
	while (r_child) {
		auto l_child = l_node.GetChildMutable(art, byte);
		if (!l_child) {
			// insert child at empty byte
			InsertChild(art, l_node, byte, *r_child);
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
		r_child = r_node.GetNextChildMutable(art, byte);
	}

	Free(art, r_node);
	return true;
}

//===--------------------------------------------------------------------===//
// Vacuum
//===--------------------------------------------------------------------===//

void Node::Vacuum(ART &art, const ARTFlags &flags) {

	D_ASSERT(HasMetadata());

	auto node_type = GetType();
	auto node_type_idx = static_cast<uint8_t>(node_type);

	// iterative functions
	if (node_type == NType::PREFIX) {
		return Prefix::Vacuum(art, *this, flags);
	}
	if (node_type == NType::LEAF_INLINED) {
		return;
	}
	if (node_type == NType::LEAF) {
		if (flags.vacuum_flags[node_type_idx - 1]) {
			Leaf::Vacuum(art, *this);
		}
		return;
	}

	auto &allocator = GetAllocator(art, node_type);
	auto needs_vacuum = flags.vacuum_flags[node_type_idx - 1] && allocator.NeedsVacuum(*this);
	if (needs_vacuum) {
		*this = allocator.VacuumPointer(*this);
		SetMetadata(node_type_idx);
	}

	// recursive functions
	switch (node_type) {
	case NType::NODE_4:
		return RefMutable<Node4>(art, *this, NType::NODE_4).Vacuum(art, flags);
	case NType::NODE_16:
		return RefMutable<Node16>(art, *this, NType::NODE_16).Vacuum(art, flags);
	case NType::NODE_48:
		return RefMutable<Node48>(art, *this, NType::NODE_48).Vacuum(art, flags);
	case NType::NODE_256:
		return RefMutable<Node256>(art, *this, NType::NODE_256).Vacuum(art, flags);
	default:
		throw InternalException("Invalid node type for Vacuum.");
	}
}

} // namespace duckdb
