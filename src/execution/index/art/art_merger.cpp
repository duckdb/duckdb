#include "duckdb/execution/index/art/art_merger.hpp"

#include "duckdb/common/swap.hpp"
#include "duckdb/execution/index/art/base_leaf.hpp"
#include "duckdb/execution/index/art/node256_leaf.hpp"
#include "duckdb/execution/index/art/prefix.hpp"
#include "duckdb/execution/index/art/base_node.hpp"

namespace duckdb {

ARTMergeResult ARTMerger::Merge() {
	while (!s.empty()) {
		auto entry = s.top();
		auto &left = entry.left;
		auto &right = entry.right;

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
		throw InternalException("invalid node type for GetBytes: %s", EnumUtil::ToString(type));
	}
}

void ARTMerger::MergeLeaves(Node &left, Node &right) {
	// Copy the bytes of right into left.
	D_ASSERT(left.GetGateStatus() == GateStatus::GATE_NOT_SET);
	D_ASSERT(right.GetGateStatus() == GateStatus::GATE_NOT_SET);

	auto bytes = GetBytes(right);

	// FIXME: Obtain a reference to left once and handle the different node type combinations.
	for (idx_t i = 0; i < bytes.size(); i++) {
		Node::InsertChild(art, left, bytes[i]);
	}
	Node::Free(art, right);
}

void ARTMerger::MergeNodeAndPrefix(Node &node, Node &prefix, const uint8_t pos) {
	D_ASSERT(node.IsNode());

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
	MergeNodeAndPrefix(node, prefix, 0);
}

void ARTMerger::MergePrefixes(Node &left, Node &right) {
	Prefix l_prefix(art, left, true);
	Prefix r_prefix(art, right, true);
	const auto count = Prefix::Count(art);

	const auto max_count = MinValue(l_prefix.data[count], r_prefix.data[count]);
	optional_idx pos;
	for (idx_t i = 0; i < max_count; i++) {
		if (l_prefix.data[i] != r_prefix.data[i]) {
			pos = i;
		}
	}

	if (pos.IsValid()) {
		// Prefixes differ. We split the left prefix, and reduce the right prefix.
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
		// Prefixes match.
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
