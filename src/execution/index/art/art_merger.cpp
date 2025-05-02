#include "duckdb/execution/index/art/art_merger.hpp"

#include "duckdb/common/swap.hpp"
#include "duckdb/execution/index/art/base_leaf.hpp"
#include "duckdb/execution/index/art/node256_leaf.hpp"
#include "duckdb/execution/index/art/prefix.hpp"
#include "duckdb/execution/index/art/base_node.hpp"
#include "duckdb/execution/index/art/node48.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/art_key.hpp"

namespace duckdb {

void ARTMerger::Init(Node &left, Node &right) {
	Emplace(left, right, GateStatus::GATE_NOT_SET, 0);
}

ARTConflictType ARTMerger::Merge() {
	while (!s.empty()) {
		// Copy the entry so we can pop it.
		auto entry = s.top();
		s.pop();

		const auto left_type = entry.left.GetType();
		const auto right_type = entry.right.GetType();

		// Early-out due to a constraint violation.
		// If right is LEAF_INLINED, then left is also LEAF_INLINED.
		const auto duplicate_key =
		    right_type == NType::LEAF_INLINED || entry.right.GetGateStatus() == GateStatus::GATE_SET;
		if (art.IsUnique() && duplicate_key) {
			return ARTConflictType::CONSTRAINT;
		}

		if (left_type == NType::LEAF_INLINED) {
			// Both left and right are inlined leaves.
			D_ASSERT(right_type == NType::LEAF_INLINED);
			MergeInlined(entry);
			continue;
		}

		if (right_type == NType::LEAF_INLINED) {
			// Left is any node except LEAF_INLINED, right is LEAF_INLINED.
			auto result = MergeNodeAndInlined(entry);
			if (result != ARTConflictType::NO_CONFLICT) {
				return result;
			}
			continue;
		}

		if (entry.right.IsLeafNode()) {
			// Both left and right are leaf nodes.
			D_ASSERT(entry.left.IsLeafNode());
			MergeLeaves(entry);
			continue;
		}

		if (entry.left.IsNode() && entry.right.IsNode()) {
			// Both left and right are nodes.
			MergeNodes(entry);
			continue;
		}

		D_ASSERT(right_type == NType::PREFIX);
		if (left_type == NType::PREFIX) {
			// Both left and right are prefixes.
			MergePrefixes(entry);
			continue;
		}
		// Left is a node, right is a PREFIX.
		MergeNodeAndPrefix(entry.left, entry.right, entry.status, entry.depth);
	}

	// We exhausted the stack.
	return ARTConflictType::NO_CONFLICT;
}

void ARTMerger::Emplace(Node &left, Node &right, const GateStatus parent_status, const idx_t depth) {
	const auto left_type = left.GetType();
	const auto right_type = right.GetType();

	if (left_type == NType::LEAF_INLINED) {
		swap(left, right);
	} else if (left_type == NType::PREFIX && right_type != NType::LEAF_INLINED) {
		swap(left, right);
	}

	// left ONLY has GATE_SET, if it is the gate node.
	// When outside the gate, we propagate the parent_status (GATE_NOT_SET) and the depth.
	// When inside the gate, we already reset the depth, and we propagate the parent_status (GATE_SET).
	if (left.GetGateStatus() == GateStatus::GATE_NOT_SET) {
		s.emplace(left, right, parent_status, depth);
		return;
	}

	// Enter a gate.
	// Reset the depth.
	D_ASSERT(parent_status == GateStatus::GATE_NOT_SET);
	s.emplace(left, right, GateStatus::GATE_SET, 0);
}

void ARTMerger::MergeInlined(NodeEntry &entry) {
	D_ASSERT(entry.left.GetType() == NType::LEAF_INLINED);
	D_ASSERT(entry.right.GetType() == NType::LEAF_INLINED);

	auto new_status = GateStatus::GATE_NOT_SET;
	if (entry.status == GateStatus::GATE_NOT_SET) {
		// Case 1: We are outside a nested leaf,
		// so we create a nested leaf.
		new_status = GateStatus::GATE_SET;
		entry.depth = 0;
	}
	// Otherwise, case 2: we are in a nested leaf with two 'compressed' prefixes.
	// A 'compressed prefix' is an inlined leaf that could've been expanded to
	// a prefix with an inlined leaf as its only child.

	// Get the corresponding row IDs and their ART keys.
	auto left_row_id = entry.left.GetRowId();
	auto right_row_id = entry.right.GetRowId();
	auto left_key = ARTKey::CreateARTKey<row_t>(arena, left_row_id);
	auto right_key = ARTKey::CreateARTKey<row_t>(arena, right_row_id);

	auto pos = left_key.GetMismatchPos(right_key, entry.depth);

	entry.left.Clear();
	reference<Node> node(entry.left);
	if (pos != entry.depth) {
		// The row IDs share a prefix.
		Prefix::New(art, node, left_key, entry.depth, pos - entry.depth);
	}

	auto left_byte = left_key.data[pos];
	auto right_byte = right_key.data[pos];

	if (pos == Prefix::ROW_ID_COUNT) {
		// The row IDs differ on the last byte.
		Node7Leaf::New(art, node);
		Node7Leaf::InsertByte(art, node, left_byte);
		Node7Leaf::InsertByte(art, node, right_byte);
		entry.left.SetGateStatus(new_status);
		return;
	}

	// Create and insert the (compressed) children.
	// We inline directly into the node, instead of creating prefixes
	// with a single inlined leaf as their child.
	Node4::New(art, node);

	Node left_child;
	Leaf::New(left_child, left_row_id);
	Node4::InsertChild(art, node, left_byte, left_child);

	Node right_child;
	Leaf::New(right_child, right_row_id);
	Node4::InsertChild(art, node, right_byte, right_child);

	entry.left.SetGateStatus(new_status);
}

ARTConflictType ARTMerger::MergeNodeAndInlined(NodeEntry &entry) {
	D_ASSERT(entry.right.GetType() == NType::LEAF_INLINED);
	D_ASSERT(entry.status == GateStatus::GATE_SET);

	// We fall back to the ART insertion code.
	auto row_id_key = ARTKey::CreateARTKey<row_t>(arena, entry.right.GetRowId());
	return art.Insert(entry.left, row_id_key, entry.depth, row_id_key, GateStatus::GATE_SET, nullptr,
	                  IndexAppendMode::DEFAULT);
}

array_ptr<uint8_t> ARTMerger::GetBytes(Node &leaf) {
	const auto type = leaf.GetType();
	switch (type) {
	case NType::NODE_7_LEAF:
		return Node::Ref<Node7Leaf>(art, leaf, type).GetBytes();
	case NType::NODE_15_LEAF:
		return Node::Ref<Node15Leaf>(art, leaf, type).GetBytes();
	case NType::NODE_256_LEAF:
		return Node::Ref<Node256Leaf>(art, leaf, type).GetBytes(arena);
	default:
		throw InternalException("invalid node type for ARTMerger::GetBytes: %s", EnumUtil::ToString(type));
	}
}

void ARTMerger::MergeLeaves(NodeEntry &entry) {
	D_ASSERT(entry.left.IsLeafNode());
	D_ASSERT(entry.right.IsLeafNode());
	D_ASSERT(entry.left.GetGateStatus() == GateStatus::GATE_NOT_SET);
	D_ASSERT(entry.right.GetGateStatus() == GateStatus::GATE_NOT_SET);

	// Merge the smaller leaf into the bigger leaf.
	if (entry.left.GetType() < entry.right.GetType()) {
		swap(entry.left, entry.right);
	}

	// Get the bytes of the right node.
	// Then, copy them into left.
	auto bytes = GetBytes(entry.right);

	// FIXME: Obtain a reference to left once and
	// FIXME: handle the different node type combinations.
	for (idx_t i = 0; i < bytes.size(); i++) {
		Node::InsertChild(art, entry.left, bytes[i]);
	}
	Node::Free(art, entry.right);
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

void ARTMerger::MergeNodes(NodeEntry &entry) {
	D_ASSERT(entry.left.IsNode());
	D_ASSERT(entry.right.IsNode());

	// Merge the smaller node into the bigger node.
	if (entry.left.GetType() < entry.right.GetType()) {
		swap(entry.left, entry.right);
	}

	// Get the children of the right node.
	// Then, copy them into left.
	auto children = ExtractChildren(entry.right);
	// As long as the arena is valid,
	// the copied-out nodes (and their references) are valid.
	Node::Free(art, entry.right);

	// First, we iterate and insert children.
	// This might grow the node, so we need to do it prior to Emplace.
	vector<idx_t> remaining;
	for (idx_t i = 0; i < children.bytes.size(); i++) {
		const auto byte = children.bytes[i];
		auto child = entry.left.GetChildMutable(art, byte);

		if (!child) {
			// There is no child at this byte.
			// We can insert the right node's child at byte and are done.
			auto &right_child = children.children[i];
			Node::InsertChild(art, entry.left, byte, right_child);
			continue;
		}
		// There is a left and a right child at this byte.
		// We remember to emplace the two children.
		remaining.emplace_back(i);
	}

	// Emplace all remaining children.
	for (idx_t i = 0; i < remaining.size(); i++) {
		const auto byte = children.bytes[remaining[i]];
		auto &right_child = children.children[remaining[i]];
		auto child = entry.left.GetChildMutable(art, byte);
		Emplace(*child, right_child, entry.status, entry.depth + 1);
	}
}

void ARTMerger::MergeNodeAndPrefix(Node &node, Node &prefix, const GateStatus parent_status, const idx_t parent_depth,
                                   const uint8_t pos) {
	D_ASSERT(node.IsNode());
	D_ASSERT(prefix.GetType() == NType::PREFIX);

	// Get the child at the prefix byte, or nullptr, if there is no child.
	const auto byte = Prefix::GetByte(art, prefix, pos);
	auto child = node.GetChildMutable(art, byte);

	// Reduce the prefix to the bytes after pos.
	// We always reduce by at least one byte,
	// thus, if the prefix was a gate, it no longer is.
	prefix.SetGateStatus(GateStatus::GATE_NOT_SET);
	Prefix::Reduce(art, prefix, pos);

	if (child) {
		// Iterate on the child and the remaining prefix.
		Emplace(*child, prefix, parent_status, parent_depth + 1);
		return;
	}

	// There is no child at this prefix byte,
	// so we can insert the remaining prefix and are done.
	Node::InsertChild(art, node, byte, prefix);
	prefix.Clear();
}

void ARTMerger::MergeNodeAndPrefix(Node &node, Node &prefix, const GateStatus parent_status, const idx_t parent_depth) {
	D_ASSERT(node.IsNode());
	D_ASSERT(prefix.GetType() == NType::PREFIX);

	MergeNodeAndPrefix(node, prefix, parent_status, parent_depth, 0);
}

void ARTMerger::MergePrefixes(NodeEntry &entry) {
	D_ASSERT(entry.left.GetType() == NType::PREFIX);
	D_ASSERT(entry.right.GetType() == NType::PREFIX);

	// We traverse prefixes until we
	// Case 1: find a position where they differ.
	// Case 2: find that they are the same.
	// Case 3: find that one prefix contains the other.

	// Until we reach one of these cases, we keep reducing
	// the right prefix (and freeing the fully reduced nodes).
	// We can do so because up to any of these three cases,
	// the prefixes are the same. That means, we only need to keep
	// one of them around.

	Prefix l_prefix(art, entry.left, true);
	Prefix r_prefix(art, entry.right, true);
	const auto count = Prefix::Count(art);

	// Find a byte at pos where the prefixes differ.
	// If they match up to max_count, then pos stays invalid.
	const auto max_count = MinValue(l_prefix.data[count], r_prefix.data[count]);
	optional_idx pos;
	for (idx_t i = 0; i < max_count; i++) {
		if (l_prefix.data[i] != r_prefix.data[i]) {
			pos = i;
			break;
		}
	}

	if (pos.IsValid()) {
		// The prefixes differ at pos.
		// We split the left prefix, and reduce the right prefix.
		// Then, we insert both remainders into a new Node4.
		// Then, we are done.
		const auto cast_pos = UnsafeNumericCast<uint8_t>(pos.GetIndex());
		const auto l_byte = Prefix::GetByte(art, entry.left, cast_pos);
		const auto r_byte = Prefix::GetByte(art, entry.right, cast_pos);

		// Split and reduce.
		reference<Node> ref(entry.left);
		Node l_child;
		const auto status = Prefix::Split(art, ref, l_child, cast_pos);
		Prefix::Reduce(art, entry.right, cast_pos);

		Node4::New(art, ref);
		ref.get().SetGateStatus(status);

		Node4::InsertChild(art, ref, l_byte, l_child);
		Node4::InsertChild(art, ref, r_byte, entry.right);
		entry.right.Clear();
		return;
	}

	if (l_prefix.data[count] == r_prefix.data[count]) {
		// The prefixes match.
		// Free the right prefix, but keep the reference to its child alive.
		// Then, iterate on the left and right (reduced) child.
		auto r_child = *r_prefix.ptr;
		r_prefix.ptr->Clear();
		Node::Free(art, entry.right);
		entry.right = r_child;

		auto depth = entry.depth + l_prefix.data[count];
		Emplace(*l_prefix.ptr, entry.right, entry.status, depth);
		return;
	}

	// max_count indexes the byte after the exhausted prefix in a child node.
	if (r_prefix.data[count] == max_count) {
		// We exhausted the right prefix.
		// Ensure that we continue merging into left.
		swap(entry.left, entry.right);
		MergeNodeAndPrefix(*r_prefix.ptr, entry.right, entry.status, entry.depth + max_count, max_count);
		return;
	}

	// We exhausted the left prefix.
	MergeNodeAndPrefix(*l_prefix.ptr, entry.right, entry.status, entry.depth + max_count, max_count);
}

} // namespace duckdb
