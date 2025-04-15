#include "duckdb/execution/index/art/art_merger.hpp"

#include "duckdb/common/swap.hpp"
#include "duckdb/execution/index/art/base_leaf.hpp"
#include "duckdb/execution/index/art/node256_leaf.hpp"
#include "duckdb/execution/index/art/prefix.hpp"
#include "duckdb/execution/index/art/base_node.hpp"
#include "duckdb/execution/index/art/node48.hpp"

namespace duckdb {

ARTMergeResult ARTMerger::Merge() {
	while (!s.empty()) {
		auto entry = s.top();
		auto &left = entry.left;
		auto &right = entry.right;

		// We have the references to the nodes, so we can now pop this entry.
		s.pop();

		const auto left_type = left.GetType();
		const auto right_type = right.GetType();

		// Early-out due to a constraint violation.
		// The 'smaller' node is always on the right, i.e., if right is any leaf, then left is any leaf, too.
		// TODO: Handle this case before pushing the entry?
		const auto duplicate_key = right_type == NType::LEAF_INLINED || right.GetGateStatus() == GateStatus::GATE_SET;
		if (art.IsUnique() && duplicate_key) {
			return ARTMergeResult::DUPLICATE;
		}

		if (left_type == NType::LEAF_INLINED) {
			// Both left and right are inlined leaves.
			D_ASSERT(right_type == NType::LEAF_INLINED);
			MergeInlined(left, right);
			continue;
		}

		if (right_type == NType::LEAF_INLINED) {
			// Left is a gate, right is LEAF_INLINED.
			D_ASSERT(left.GetGateStatus() == GateStatus::GATE_SET);
			MergeGateAndInlined(left, right);
			continue;
		}

		if (right.IsLeafNode()) {
			// Both left and right are leaf nodes.
			D_ASSERT(left.IsLeafNode());
			MergeLeaves(left, right);
			continue;
		}

		if (left.IsNode() && right.IsNode()) {
			// Both left and right are nodes.
			MergeNodes(left, right);
			continue;
		}

		D_ASSERT(right_type == NType::PREFIX);
		if (left_type == NType::PREFIX) {
			// Both left and right are prefixes.
			MergePrefixes(left, right);
			continue;
		}
		// Left is a node, right is a PREFIX.
		MergeNodeAndPrefix(left, right);
	}

	// We exhausted the stack.
	return ARTMergeResult::SUCCESS;
}

void ARTMerger::Emplace(Node &left, Node &right) {
	const auto left_type = left.GetType();
	if (left_type == NType::LEAF_INLINED || left_type == NType::PREFIX) {
		swap(left, right);
	}
	s.emplace(left, right);
}

array_ptr<uint8_t> ARTMerger::GetBytes(Node &leaf_node) {
	const auto type = leaf_node.GetType();
	switch (type) {
	case NType::NODE_7_LEAF:
		return Node::Ref<Node7Leaf>(art, leaf_node, type).GetBytes();
	case NType::NODE_15_LEAF:
		return Node::Ref<Node15Leaf>(art, leaf_node, type).GetBytes();
	case NType::NODE_256_LEAF:
		return Node::Ref<Node256Leaf>(art, leaf_node, type).GetBytes(arena);
	default:
		throw InternalException("invalid node type for ARTMerger::GetBytes: %s", EnumUtil::ToString(type));
	}
}

void ARTMerger::MergeLeaves(Node &left, Node &right) {
	D_ASSERT(left.IsLeafNode());
	D_ASSERT(right.IsLeafNode());
	D_ASSERT(left.GetGateStatus() == GateStatus::GATE_NOT_SET);
	D_ASSERT(right.GetGateStatus() == GateStatus::GATE_NOT_SET);

	// Get the bytes of the right node.
	// Then, copy them into left.
	auto bytes = GetBytes(right);

	// FIXME: Obtain a reference to left once and
	// FIXME: handle the different node type combinations.
	for (idx_t i = 0; i < bytes.size(); i++) {
		Node::InsertChild(art, left, bytes[i]);
	}
	Node::Free(art, right);
}

NodeChildren ARTMerger::ExtractChildren(Node &node) {
	const auto type = node.GetType();
	switch (type) {
	case NType::NODE_4:
		return Node::Ref<Node4>(art, node, type).ExtractChildren(arena);
	case NType::NODE_16:
		return Node::Ref<Node16>(art, node, type).ExtractChildren(arena);
	case NType::NODE_48:
		return Node::Ref<Node48>(art, node, type).ExtractChildren(arena);
	case NType::NODE_256:
		return Node::Ref<Node256>(art, node, type).ExtractChildren(arena);
	default:
		throw InternalException("invalid node type for ARTMerger::GetChildren: %s", EnumUtil::ToString(type));
	}
}

void ARTMerger::MergeNodes(Node &left, Node &right) {
	D_ASSERT(left.IsNode());
	D_ASSERT(right.IsNode());

	// Merge the smaller node into the bigger node.
	if (left.GetType() < right.GetType()) {
		swap(left, right);
	}

	// Get the children of the right node.
	// Then, copy them into left.
	auto children = ExtractChildren(right);

	// FIXME: Obtain a reference to left once and
	// FIXME: handle the different node type combinations.
	for (idx_t i = 0; i < children.bytes.size(); i++) {
		const auto byte = children.bytes[i];
		auto &right_child = children.children[i];
		auto child = left.GetChildMutable(art, byte);

		if (!child) {
			Node::InsertChild(art, left, byte, right_child);
			continue;
		}
		Emplace(*child, right_child);
	}

	// As long as the arena is valid,
	// the copied-out nodes (and their references) are valid.
	Node::Free(art, right);
}

void ARTMerger::MergeNodeAndPrefix(Node &node, Node &prefix, const uint8_t pos) {
	D_ASSERT(node.IsNode());
	D_ASSERT(prefix.GetType() == NType::PREFIX);

	// Get the child at the prefix byte, or nullptr, if there is no child.
	const auto byte = Prefix::GetByte(art, prefix, pos);
	auto child = node.GetChildMutable(art, byte);

	// Reduce the prefix to the bytes after pos.
	Prefix::Reduce(art, prefix, pos);

	if (child) {
		// Iterate on the child and the remaining prefix.
		Emplace(*child, prefix);
		return;
	}

	Node::InsertChild(art, node, byte, prefix);
	prefix.Clear();
}

void ARTMerger::MergeNodeAndPrefix(Node &node, Node &prefix) {
	D_ASSERT(node.IsNode());
	D_ASSERT(prefix.GetType() == NType::PREFIX);

	MergeNodeAndPrefix(node, prefix, 0);
}

void ARTMerger::MergePrefixes(Node &left, Node &right) {
	D_ASSERT(left.GetType() == NType::PREFIX);
	D_ASSERT(right.GetType() == NType::PREFIX);

	// We traverse prefixes until we
	// - 3.1. find a position where they differ.
	// - 3.2. find that they are the same.
	// - 3.3. find that one prefix contains the other.

	// Until we reach one of these cases, we keep reducing
	// the right prefix (and freeing the fully reduced nodes).
	// We can do so because up to any of these three cases,
	// the prefixes are the same. That means, we only need to keep
	// one of them around (as we are merging).

	Prefix l_prefix(art, left, true);
	Prefix r_prefix(art, right, true);
	const auto count = Prefix::Count(art);

	// Find a byte at pos where the prefixes differ.
	// If they match up to max_count, then pos stays invalid.
	const auto max_count = MinValue(l_prefix.data[count], r_prefix.data[count]);
	optional_idx pos;
	for (idx_t i = 0; i < max_count; i++) {
		if (l_prefix.data[i] != r_prefix.data[i]) {
			pos = i;
		}
	}

	if (pos.IsValid()) {
		// The prefixes differ at pos.
		// We split the left prefix, and reduce the right prefix.
		// Then, we insert both remainders into a new Node4.
		const auto cast_pos = UnsafeNumericCast<uint8_t>(pos.GetIndex());

		Node l_child;
		const auto l_byte = Prefix::GetByte(art, left, cast_pos);

		reference<Node> ref(left);
		const auto status = Prefix::Split(art, ref, l_child, cast_pos);
		Node4::New(art, ref);
		ref.get().SetGateStatus(status);

		Node4::InsertChild(art, ref, l_byte, l_child);

		auto r_byte = Prefix::GetByte(art, right, cast_pos);
		Prefix::Reduce(art, right, cast_pos);
		Node4::InsertChild(art, ref, r_byte, right);
		right.Clear();
		return;
	}

	if (l_prefix.data[count] == r_prefix.data[count]) {
		// The prefixes match.
		// Free the right prefix, but keep the reference to its child alive.
		auto r_child = *r_prefix.ptr;
		r_prefix.ptr->Clear();
		Node::Free(art, right);
		right = r_child;

		Emplace(*l_prefix.ptr, right);
		return;
	}

	if (r_prefix.data[count] == max_count) {
		// We exhausted the right prefix.
		// Ensure that we continue merging into left.
		swap(left, right);
		MergeNodeAndPrefix(*r_prefix.ptr, right, max_count);
		return;
	}

	// We exhausted the left prefix.
	MergeNodeAndPrefix(*l_prefix.ptr, right, max_count);
}

} // namespace duckdb
