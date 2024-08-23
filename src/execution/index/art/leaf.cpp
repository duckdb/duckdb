#include "duckdb/execution/index/art/leaf.hpp"

#include "duckdb/common/types.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/base_leaf.hpp"
#include "duckdb/execution/index/art/base_node.hpp"
#include "duckdb/execution/index/art/iterator.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/execution/index/art/prefix.hpp"

namespace duckdb {

void Leaf::New(Node &node, const row_t row_id) {
	D_ASSERT(row_id < MAX_ROW_ID_LOCAL);

	auto status = node.GetGateStatus();
	node.Clear();

	node.SetMetadata(static_cast<uint8_t>(INLINED));
	node.SetRowId(row_id);
	node.SetGateStatus(status);
}

void Leaf::New(ART &art, reference<Node> &node, const unsafe_vector<ARTKey> &row_ids, const idx_t start,
               const idx_t count) {
	D_ASSERT(count > 1);
	D_ASSERT(!node.get().HasMetadata());

	// We cannot recurse into the leaf during Construct(...) because row IDs are not sorted.
	for (idx_t i = 0; i < count; i++) {
		idx_t offset = start + i;
		art.Insert(node, row_ids[offset], 0, row_ids[offset], GateStatus::GATE_SET);
	}
	node.get().SetGateStatus(GateStatus::GATE_SET);
}

void Leaf::MergeInlined(ART &art, Node &l_node, Node &r_node) {
	D_ASSERT(r_node.GetType() == INLINED);

	ArenaAllocator arena_allocator(Allocator::Get(art.db));
	auto key = ARTKey::CreateARTKey<row_t>(arena_allocator, r_node.GetRowId());
	art.Insert(l_node, key, 0, key, l_node.GetGateStatus());
	r_node.Clear();
}

void Leaf::InsertIntoInlined(ART &art, Node &node, const ARTKey &row_id, idx_t depth, const GateStatus status) {
	D_ASSERT(node.GetType() == INLINED);

	ArenaAllocator allocator(Allocator::Get(art.db));
	auto key = ARTKey::CreateARTKey<row_t>(allocator, node.GetRowId());

	GateStatus new_status;
	if (status == GateStatus::GATE_NOT_SET || node.GetGateStatus() == GateStatus::GATE_SET) {
		new_status = GateStatus::GATE_SET;
	} else {
		new_status = GateStatus::GATE_NOT_SET;
	}

	if (new_status == GateStatus::GATE_SET) {
		depth = 0;
	}
	node.Clear();

	// Get the mismatching position.
	D_ASSERT(row_id.len == key.len);
	auto pos = row_id.GetMismatchPos(key, depth);
	D_ASSERT(pos != DConstants::INVALID_INDEX);
	D_ASSERT(pos >= depth);
	auto byte = row_id.data[pos];

	// Create the (optional) prefix and the node.
	reference<Node> next(node);
	auto count = pos - depth;
	if (count != 0) {
		Prefix::New(art, next, row_id, depth, count);
	}
	if (pos == Prefix::ROW_ID_COUNT) {
		Node7Leaf::New(art, next);
	} else {
		Node4::New(art, next);
	}

	// Create the children.
	Node row_id_node;
	Leaf::New(row_id_node, row_id.GetRowId());
	Node remainder;
	if (pos != Prefix::ROW_ID_COUNT) {
		Leaf::New(remainder, key.GetRowId());
	}

	Node::InsertChild(art, next, key[pos], remainder);
	Node::InsertChild(art, next, byte, row_id_node);
	node.SetGateStatus(new_status);
}

void Leaf::TransformToNested(ART &art, Node &node) {
	D_ASSERT(node.GetType() == LEAF);

	ArenaAllocator allocator(Allocator::Get(art.db));
	Node root = Node();

	// Move all row IDs into the nested leaf.
	reference<const Node> leaf_ref(node);
	while (leaf_ref.get().HasMetadata()) {
		auto &leaf = Node::Ref<const Leaf>(art, leaf_ref, LEAF);
		for (uint8_t i = 0; i < leaf.count; i++) {
			auto row_id = ARTKey::CreateARTKey<row_t>(allocator, leaf.row_ids[i]);
			art.Insert(root, row_id, 0, row_id, GateStatus::GATE_SET);
		}
		leaf_ref = leaf.ptr;
	}

	root.SetGateStatus(GateStatus::GATE_SET);
	Node::Free(art, node);
	node = root;
}

void Leaf::TransformToDeprecated(ART &art, Node &node) {
	D_ASSERT(node.GetGateStatus() == GateStatus::GATE_SET || node.GetType() == LEAF);

	// Early-out, if we never transformed this leaf.
	if (node.GetGateStatus() == GateStatus::GATE_NOT_SET) {
		return;
	}

	// Collect all row IDs and free the nested leaf.
	unsafe_vector<row_t> row_ids;
	Iterator it(art);
	it.FindMinimum(node);
	ARTKey empty_key = ARTKey();
	it.Scan(empty_key, NumericLimits<row_t>().Maximum(), row_ids, false);
	Node::Free(art, node);
	D_ASSERT(row_ids.size() > 1);

	// Create the deprecated leaves.
	idx_t remaining = row_ids.size();
	idx_t copy_count = 0;
	reference<Node> ref(node);
	while (remaining) {
		ref.get() = Node::GetAllocator(art, LEAF).New();
		ref.get().SetMetadata(static_cast<uint8_t>(LEAF));

		auto &leaf = Node::Ref<Leaf>(art, ref, LEAF);
		auto min = MinValue(UnsafeNumericCast<idx_t>(LEAF_SIZE), remaining);
		leaf.count = UnsafeNumericCast<uint8_t>(min);

		for (uint8_t i = 0; i < leaf.count; i++) {
			leaf.row_ids[i] = row_ids[copy_count + i];
		}

		copy_count += leaf.count;
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
		Node::GetAllocator(art, LEAF).Free(node);
		node = next;
	}
	node.Clear();
}

bool Leaf::DeprecatedGetRowIds(ART &art, const Node &node, unsafe_vector<row_t> &row_ids, const idx_t max_count) {
	D_ASSERT(node.GetType() == LEAF);

	reference<const Node> ref(node);
	while (ref.get().HasMetadata()) {

		auto &leaf = Node::Ref<const Leaf>(art, ref, LEAF);
		if (row_ids.size() + leaf.count > max_count) {
			return false;
		}
		for (uint8_t i = 0; i < leaf.count; i++) {
			row_ids.push_back(leaf.row_ids[i]);
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

string Leaf::DeprecatedVerifyAndToString(ART &art, const Node &node, const bool only_verify) {
	D_ASSERT(node.GetType() == LEAF);

	string str = "";
	reference<const Node> ref(node);

	while (ref.get().HasMetadata()) {
		auto &leaf = Node::Ref<const Leaf>(art, ref, LEAF);
		D_ASSERT(leaf.count <= LEAF_SIZE);

		str += "Leaf [count: " + to_string(leaf.count) + ", row IDs: ";
		for (uint8_t i = 0; i < leaf.count; i++) {
			str += to_string(leaf.row_ids[i]) + "-";
		}
		str += "] ";
		ref = leaf.ptr;
	}

	return only_verify ? "" : str;
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
