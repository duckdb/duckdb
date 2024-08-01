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

bool Node::Merge(ART &art, Node &other, bool in_gate) {
	if (HasMetadata()) {
		return MergeInternal(art, other, in_gate);
	}

	*this = other;
	other = Node();
	return true;
}

bool Node::PrefixContainsOther(ART &art, Node &l_node, Node &r_node, uint8_t mismatch_pos, bool in_gate) {
	// r_node's prefix contains l_node's prefix.
	// l_node must be a node allowing child nodes.
	D_ASSERT(l_node.IsNode());

	// Check if the next byte (mismatch_pos) in r_node exists in l_node.
	auto mismatch_byte = Prefix::GetByte(art, r_node, mismatch_pos);
	auto child_node = l_node.GetChildMutable(art, mismatch_byte);

	// Reduce r_node's prefix to the bytes after mismatch_pos.
	Prefix::Reduce(art, r_node, mismatch_pos);

	if (child_node) {
		// Recurse into the child of l_node and the remaining prefix of r_node.
		return child_node->MergeInternal(art, r_node, in_gate);
	}

	// Insert r_node as a child of l_node at the empty position.
	Node::InsertChild(art, l_node, mismatch_byte, r_node);
	r_node.Clear();
	return true;
}

void Node::MergeIntoNode4(ART &art, Node &l_node, Node &r_node, uint8_t pos) {
	Node l_child;
	auto l_byte = Prefix::GetByte(art, l_node, pos);

	reference<Node> l_node_ref(l_node);
	auto freed_gate = Prefix::Split(art, l_node_ref, l_child, pos);

	Node4::New(art, l_node_ref);
	if (freed_gate) {
		l_node_ref.get().SetGate();
	}

	// Insert the children.
	Node4::InsertChild(art, l_node_ref, l_byte, l_child);
	auto r_byte = Prefix::GetByte(art, r_node, pos);
	Prefix::Reduce(art, r_node, pos);
	Node4::InsertChild(art, l_node_ref, r_byte, r_node);
	r_node.Clear();
}

bool Node::MergePrefixes(ART &art, Node &other, bool in_gate) {
	reference<Node> l_node(*this);
	reference<Node> r_node(other);
	auto mismatch_pos = DConstants::INVALID_INDEX;

	if (l_node.get().IsPrefix() && r_node.get().IsPrefix()) {
		// Traverse prefixes. Possibly change the referenced nodes.
		if (!Prefix::Traverse(art, l_node, r_node, mismatch_pos, in_gate)) {
			return false;
		}
		if (mismatch_pos == DConstants::INVALID_INDEX) {
			return true;
		}

	} else {
		// l_prefix contains r_prefix
		if (l_node.get().IsPrefix()) {
			swap(*this, other);
		}
		mismatch_pos = 0;
	}
	D_ASSERT(mismatch_pos != DConstants::INVALID_INDEX);

	// l_prefix contains r_prefix
	if (!l_node.get().IsPrefix() && r_node.get().IsPrefix()) {
		return PrefixContainsOther(art, l_node, r_node, UnsafeNumericCast<uint8_t>(mismatch_pos), in_gate);
	}

	// The prefixes differ.
	MergeIntoNode4(art, l_node, r_node, UnsafeNumericCast<uint8_t>(mismatch_pos));
	return true;
}

bool Node::MergeInternal(ART &art, Node &other, bool in_gate) {
	D_ASSERT(HasMetadata());
	D_ASSERT(other.HasMetadata());

	// Merge inlined leaves.
	if (GetType() == NType::LEAF_INLINED) {
		swap(*this, other);
	}
	if (other.GetType() == NType::LEAF_INLINED) {
		D_ASSERT(!in_gate);
		D_ASSERT(other.IsGate() || other.GetType() == NType::LEAF_INLINED);
		if (art.IsUnique()) {
			return false;
		}
		Leaf::MergeInlined(art, *this, other);
		return true;
	}

	// Enter a gate.
	if (IsGate() && other.IsGate() && !in_gate) {
		return Merge(art, other, true);
	}

	// Merge N4, N16, N48, N256 nodes.
	if (IsNode() && other.IsNode()) {
		return MergeNodes(art, other, in_gate);
	}
	// Merge N7, N15, N256 leaf nodes.
	if (IsLeafNode() && other.IsLeafNode()) {
		D_ASSERT(in_gate);
		return MergeNodes(art, other, in_gate);
	}

	// Merge prefixes.
	return MergePrefixes(art, other, in_gate);
}

bool MergeNormalNodes(ART &art, Node &l_node, Node &r_node, Node &empty_node, uint8_t byte, bool in_gate) {
	// Merge N4, N16, N48, N256 nodes.
	D_ASSERT(l_node.IsNode() && r_node.IsNode());
	D_ASSERT(l_node.IsGate() && r_node.IsGate() || !l_node.IsGate() && !r_node.IsGate());

	auto r_child = r_node.GetNextChildMutable(art, byte);
	while (r_child) {
		auto l_child = l_node.GetChildMutable(art, byte);
		if (!l_child) {
			Node::InsertChild(art, l_node, byte, *r_child);
			r_node.ReplaceChild(art, byte, empty_node);
		} else {
			if (!l_child->MergeInternal(art, *r_child, in_gate)) {
				return false;
			}
		}

		if (byte == NumericLimits<uint8_t>::Maximum()) {
			break;
		}
		byte++;
		r_child = r_node.GetNextChildMutable(art, byte);
	}

	Node::Free(art, r_node);
	return true;
}

void MergeLeafNodes(ART &art, Node &l_node, Node &r_node, Node &empty_node, uint8_t byte) {
	// Merge N7, N15, N256 leaf nodes.
	D_ASSERT(l_node.IsLeafNode() && r_node.IsLeafNode());
	D_ASSERT(!l_node.IsGate() && !r_node.IsGate());

	auto has_next_byte = r_node.GetNextByte(art, byte);
	while (has_next_byte) {
		// Row IDs are always unique.
		Node::InsertChild(art, l_node, byte, empty_node);
		if (byte == NumericLimits<uint8_t>::Maximum()) {
			break;
		}
		byte++;
		has_next_byte = r_node.GetNextByte(art, byte);
	}

	Node::Free(art, r_node);
}

bool Node::MergeNodes(ART &art, Node &other, bool in_gate) {
	// Merge the smaller node into the bigger node.
	if (GetType() < other.GetType()) {
		swap(*this, other);
	}

	Node empty_node = Node();
	if (IsNode()) {
		return MergeNormalNodes(art, *this, other, empty_node, 0, in_gate);
	}
	MergeLeafNodes(art, *this, other, empty_node, 0);
	return true;
}

} // namespace duckdb
