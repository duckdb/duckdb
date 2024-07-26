#include "duckdb/execution/index/art/node.hpp"

#include "duckdb/common/swap.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/node256.hpp"
#include "duckdb/execution/index/art/node4.hpp"
#include "duckdb/execution/index/art/node48.hpp"
#include "duckdb/execution/index/art/prefix.hpp"

namespace duckdb {

void Node::InitializeMerge(ART &art, const ARTFlags &flags) {
	D_ASSERT(HasMetadata());

	auto type = GetType();
	switch (type) {
	case NType::PREFIX:
		return Prefix::InitializeMerge(art, *this, flags);
	case NType::LEAF:
		throw InternalException("Failed to initialize merge due to deprecated ART storage.");
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
	case NType::PREFIX_INLINED:
	case NType::NODE_7_LEAF:
	case NType::NODE_15_LEAF:
	case NType::NODE_256_LEAF:
		break;
	}

	IncreaseBufferId(flags.merge_buffer_counts[static_cast<uint8_t>(type) - 1]);
}

bool Node::Merge(ART &art, Node &other, const bool inside_gate) {
	if (HasMetadata()) {
		return MergeInternal(art, other, inside_gate);
	}

	*this = other;
	other = Node();
	return true;
}

bool Node::PrefixContainsOther(ART &art, Node &l_node, Node &r_node, idx_t mismatch_pos, bool inside_gate) {
	// r_node's prefix contains l_node's prefix.
	// l_node cannot be a leaf by our construction.
	D_ASSERT(!l_node.IsLeaf() && !l_node.IsGate());

	// Check if the next byte (mismatch_position) in r_node exists in l_node.
	auto mismatch_byte = Prefix::GetByte(art, r_node, mismatch_pos);
	auto child_node = l_node.GetChildMutable(art, mismatch_byte);

	// Reduce r_node's prefix to the bytes after mismatch_position.
	Prefix::Reduce(art, r_node, mismatch_pos);

	if (!child_node) {
		// Insert r_node as a child of l_node at the empty position.
		Node::InsertChild(art, l_node, mismatch_byte, r_node);
		r_node.Clear();
		return true;
	}

	// Recurse into the child of l_node and the remaining prefix of r_node.
	return child_node->MergeInternal(art, r_node, inside_gate);
}

void Node::MergeIntoNode4(ART &art, Node &l_node, Node &r_node, idx_t mismatch_pos) {
	Node l_child;
	auto l_byte = Prefix::GetByte(art, l_node, mismatch_pos);
	auto freed_gate = Prefix::Split(art, l_node, l_child, mismatch_pos);

	Node4::New(art, l_node);
	if (freed_gate) {
		l_node.SetGate();
	}

	// Insert the children.
	Node4::InsertChild(art, l_node, l_byte, l_child);
	auto r_byte = Prefix::GetByte(art, r_node, mismatch_pos);
	Prefix::Reduce(art, r_node, mismatch_pos);
	Node4::InsertChild(art, l_node, r_byte, r_node);
	r_node.Clear();
}

// TODO
bool Node::MergeInternal(ART &art, Node &other, const bool inside_gate) {
	D_ASSERT(HasMetadata());
	D_ASSERT(other.HasMetadata());

	// Resolve leaves.
	if (GetType() == NType::LEAF_INLINED) {
		D_ASSERT(other.IsGate() || other.GetType() == NType::LEAF_INLINED);
		if (art.IsUnique()) {
			return false;
		}
		Leaf::Merge(art, *this, other);
		return true;
	}
	if (other.GetType() == NType::LEAF_INLINED) {
		D_ASSERT(IsGate() || GetType() == NType::LEAF_INLINED);
		if (art.IsUnique()) {
			return false;
		}
		Leaf::Merge(art, *this, other);
		return true;
	}
	if (IsGate() && other.IsGate() && !inside_gate) {
		Leaf::Merge(art, *this, other);
		return true;
	}

	// case 1: both nodes have no prefix
	if (GetType() != NType::PREFIX && other.GetType() != NType::PREFIX) {
		return MergeNodes(art, other, inside_gate);
	}

	reference<Node> l_node(*this);
	reference<Node> r_node(other);

	auto mismatch_position = DConstants::INVALID_INDEX;

	// traverse prefixes
	if (l_node.get().GetType() == NType::PREFIX && r_node.get().GetType() == NType::PREFIX) {

		if (!Prefix::Traverse(art, l_node, r_node, mismatch_position, inside_gate)) {
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
		return PrefixContainsOther(art, l_node, r_node, mismatch_position, inside_gate);
	}

	// case 3: prefixes differ at a specific byte
	MergeIntoNode4(art, l_node, r_node, mismatch_position);
	return true;
}

// TODO
bool Node::MergeNodes(ART &art, Node &other, const bool inside_gate) {
	D_ASSERT(HasMetadata() && other.HasMetadata());
	D_ASSERT(GetType() != NType::PREFIX && other.GetType() != NType::PREFIX);
	D_ASSERT(!IsLeaf() && !IsGate());
	D_ASSERT(!other.IsLeaf() && !other.IsGate());

	// always try to merge the smaller node into the bigger node
	// because maybe there is enough free space in the bigger node to fit the smaller one
	// without too much recursion
	if (GetType() < other.GetType()) {
		swap(*this, other);
	}

	Node empty_node = Node();
	auto &l_node = *this;
	auto &r_node = other;

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
			if (!l_child->MergeInternal(art, *r_child, inside_gate)) {
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

} // namespace duckdb
