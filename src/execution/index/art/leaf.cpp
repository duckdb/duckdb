#include "duckdb/execution/index/art/leaf.hpp"

#include "duckdb/common/types.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/base_leaf.hpp"
#include "duckdb/execution/index/art/base_node.hpp"
#include "duckdb/execution/index/art/iterator.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/execution/index/art/prefix.hpp"
#include "duckdb/execution/index/art/art_operator.hpp"

namespace duckdb {

void Leaf::New(Node &node, const row_t row_id) {
	D_ASSERT(row_id < MAX_ROW_ID_LOCAL);
	node.Clear();
	node.SetMetadata(static_cast<uint8_t>(INLINED));
	node.SetRowId(row_id);
}

void Leaf::MergeInlined(ArenaAllocator &arena, ART &art, Node &left, Node &right, GateStatus status, idx_t depth) {
	D_ASSERT(left.GetType() == NType::LEAF_INLINED);
	D_ASSERT(right.GetType() == NType::LEAF_INLINED);

	status = status == GateStatus::GATE_NOT_SET ? GateStatus::GATE_SET : GateStatus::GATE_NOT_SET;
	if (status == GateStatus::GATE_SET) {
		// Case 1: We are outside a nested leaf,
		// so we create a nested leaf.
		depth = 0;
	}
	// Otherwise, case 2: we are in a nested leaf with two 'compressed' prefixes.
	// A 'compressed prefix' is an inlined leaf that could've been expanded to
	// a prefix with an inlined leaf as its only child.

	// Get the corresponding row IDs and their ART keys.
	auto left_row_id = left.GetRowId();
	auto right_row_id = right.GetRowId();
	auto left_key = ARTKey::CreateARTKey<row_t>(arena, left_row_id);
	auto right_key = ARTKey::CreateARTKey<row_t>(arena, right_row_id);

	auto pos = left_key.GetMismatchPos(right_key, depth);

	left.Clear();
	reference<Node> node(left);
	if (pos != depth) {
		// The row IDs share a prefix.
		Prefix::New(art, node, left_key, depth, pos - depth);
	}

	auto left_byte = left_key.data[pos];
	auto right_byte = right_key.data[pos];

	if (pos == Prefix::ROW_ID_COUNT) {
		// The row IDs differ on the last byte.
		Node7Leaf::New(art, node);
		Node7Leaf::InsertByte(art, node, left_byte);
		Node7Leaf::InsertByte(art, node, right_byte);
		left.SetGateStatus(status);
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

	left.SetGateStatus(status);
}

void Leaf::TransformToNested(ART &art, Node &node) {
	D_ASSERT(node.GetType() == LEAF);

	ArenaAllocator arena(Allocator::Get(art.db));
	Node root = Node();

	// Move all row IDs into the nested leaf.
	reference<const Node> leaf_ref(node);
	while (leaf_ref.get().HasMetadata()) {
		auto &leaf = Node::Ref<const Leaf>(art, leaf_ref, LEAF);
		for (uint8_t i = 0; i < leaf.count; i++) {
			auto row_id = ARTKey::CreateARTKey<row_t>(arena, leaf.row_ids[i]);
			auto conflict_type = ARTOperator::Insert(arena, art, root, row_id, 0, row_id, GateStatus::GATE_SET, nullptr,
			                                         IndexAppendMode::INSERT_DUPLICATES);
			if (conflict_type != ARTConflictType::NO_CONFLICT) {
				throw InternalException("invalid conflict type in Leaf::TransformToNested");
			}
		}
		leaf_ref = leaf.ptr;
	}

	root.SetGateStatus(GateStatus::GATE_SET);
	DeprecatedFree(art, node);
	node = root;
}

void Leaf::TransformToDeprecated(ART &art, Node &node) {
	D_ASSERT(node.GetGateStatus() == GateStatus::GATE_SET || node.GetType() == LEAF);

	// Early-out, if we never transformed this leaf.
	if (node.GetGateStatus() == GateStatus::GATE_NOT_SET) {
		return;
	}

	// Collect all row IDs and free the nested leaf.
	set<row_t> row_ids;
	Iterator it(art);
	it.FindMinimum(node);
	ARTKey empty_key = ARTKey();
	it.Scan(empty_key, NumericLimits<row_t>().Maximum(), row_ids, false);
	Node::FreeTree(art, node);
	D_ASSERT(row_ids.size() > 1);

	// Create the deprecated leaves.
	idx_t remaining = row_ids.size();
	auto row_ids_it = row_ids.begin();
	reference<Node> ref(node);
	while (remaining) {
		ref.get() = Node::GetAllocator(art, LEAF).New();
		ref.get().SetMetadata(static_cast<uint8_t>(LEAF));

		auto &leaf = Node::Ref<Leaf>(art, ref, LEAF);
		auto min = MinValue(UnsafeNumericCast<idx_t>(LEAF_SIZE), remaining);
		leaf.count = UnsafeNumericCast<uint8_t>(min);

		for (uint8_t i = 0; i < leaf.count; i++) {
			leaf.row_ids[i] = *row_ids_it;
			row_ids_it++;
		}
		remaining -= leaf.count;

		ref = leaf.ptr;
		leaf.ptr.Clear();
	}
}

//===--------------------------------------------------------------------===//
// Deprecated code paths.
//===--------------------------------------------------------------------===//

void Leaf::DeprecatedFree(ART &art, Node &node) {
	D_ASSERT(node.GetType() == LEAF);
	Node next;
	while (node.HasMetadata()) {
		next = Node::Ref<Leaf>(art, node, LEAF).ptr;
		Node::FreeNode(art, node);
		node = next;
	}
	node.Clear();
}

bool Leaf::DeprecatedGetRowIds(ART &art, const Node &node, set<row_t> &row_ids, const idx_t max_count) {
	D_ASSERT(node.GetType() == LEAF);

	reference<const Node> ref(node);
	while (ref.get().HasMetadata()) {
		auto &leaf = Node::Ref<const Leaf>(art, ref, LEAF);
		if (row_ids.size() + leaf.count > max_count) {
			return false;
		}
		for (uint8_t i = 0; i < leaf.count; i++) {
			row_ids.insert(leaf.row_ids[i]);
		}
		ref = leaf.ptr;
	}
	return true;
}

void Leaf::DeprecatedVacuum(ART &art, Node &node) {
	D_ASSERT(node.HasMetadata());
	D_ASSERT(node.GetType() == LEAF);

	auto &allocator = Node::GetAllocator(art, LEAF);
	reference<Node> ref(node);
	while (ref.get().HasMetadata()) {
		if (allocator.NeedsVacuum(ref)) {
			ref.get() = allocator.VacuumPointer(ref);
			ref.get().SetMetadata(static_cast<uint8_t>(LEAF));
		}
		auto &leaf = Node::Ref<Leaf>(art, ref, LEAF);
		ref = leaf.ptr;
	}
}

string Leaf::DeprecatedToString(ART &art, const Node &node) {
	string str = "";
	reference<const Node> ref(node);

	while (ref.get().HasMetadata()) {
		auto &leaf = Node::Ref<const Leaf>(art, ref, LEAF);

		str += "Leaf [count: " + to_string(leaf.count) + ", row IDs: ";
		for (uint8_t i = 0; i < leaf.count; i++) {
			str += to_string(leaf.row_ids[i]) + "-";
		}
		str += "] ";
		ref = leaf.ptr;
	}

	return str;
}

void Leaf::DeprecatedVerify(ART &art, const Node &node) {
	D_ASSERT(node.GetType() == LEAF);

	reference<const Node> ref(node);

	while (ref.get().HasMetadata()) {
		auto &leaf = Node::Ref<const Leaf>(art, ref, LEAF);
		D_ASSERT(leaf.count <= LEAF_SIZE);
		ref = leaf.ptr;
	}
}

void Leaf::DeprecatedVerifyAllocations(ART &art, unordered_map<uint8_t, idx_t> &node_counts) const {
	auto idx = Node::GetAllocatorIdx(LEAF);
	node_counts[idx]++;

	reference<const Node> ref(ptr);
	while (ref.get().HasMetadata()) {
		auto &leaf = Node::Ref<const Leaf>(art, ref, LEAF);
		node_counts[idx]++;
		ref = leaf.ptr;
	}
}

} // namespace duckdb
