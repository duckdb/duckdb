#include "duckdb/execution/index/art/leaf.hpp"

#include "duckdb/common/types.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/iterator.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/execution/index/art/prefix.hpp"

namespace duckdb {

void Leaf::New(Node &node, const row_t row_id) {
	D_ASSERT(row_id < MAX_ROW_ID_LOCAL);

	node.Clear();
	node.SetMetadata(static_cast<uint8_t>(NType::LEAF_INLINED));
	node.SetRowId(row_id);
}

void Leaf::New(ART &art, reference<Node> &node, unsafe_vector<ARTKey> &row_ids, idx_t start, idx_t count) {
	D_ASSERT(count > 1);
	D_ASSERT(!node.get().HasMetadata());

	ARTKeySection section(start, start + count - 1, 0, 0);
	art.ConstructInternal(row_ids, row_ids, node, section, true);
	node.get().SetGate();
}

void Leaf::MergeInlined(ART &art, Node &l_node, Node &r_node) {
	D_ASSERT(r_node.GetType() == NType::LEAF_INLINED);

	// Create an ARTKey from the row ID.
	ArenaAllocator arena_allocator(Allocator::Get(art.db));
	auto key = ARTKey::CreateARTKey<row_t>(arena_allocator, r_node.GetRowId());

	// Insert the key.
	art.Insert(l_node, key, 0, key, l_node.IsGate());
	r_node.Clear();
}

void Leaf::InsertIntoInlined(ART &art, Node &node, reference<ARTKey> row_id) {
	D_ASSERT(node.GetType() == NType::LEAF_INLINED);

	auto inlined_row_id = node.GetRowId();
	node.Clear();

	ArenaAllocator allocator(Allocator::Get(art.db));
	auto inlined_row_id_key = ARTKey::CreateARTKey<row_t>(allocator, inlined_row_id);

	// Insert both row IDs into the nested ART.
	// Row IDs are always unique.
	art.Insert(node, inlined_row_id_key, 0, inlined_row_id_key, true);
	art.Insert(node, row_id, 0, row_id, true);
	node.SetGate();
}

void Leaf::EraseFromNested(ART &art, Node &node, const ARTKey &row_id) {
	D_ASSERT(node.HasMetadata());

	art.Erase(node, row_id, 0, row_id, true);
	if (node.GetType() != NType::PREFIX_INLINED) {
		return;
	}

	// Inline the row ID.
	Prefix prefix(art, node, true);
	auto data_ptr = &prefix.data[0];
	auto remaining_row_id = ARTKey(data_ptr, sizeof(row_t)).GetRowID();
	Node::Free(art, node);
	Leaf::New(node, remaining_row_id);
}

void Leaf::TransformToNested(ART &art, Node &node) {
	D_ASSERT(node.GetType() == NType::LEAF);

	ArenaAllocator allocator(Allocator::Get(art.db));
	Node root = Node();

	// Move all row IDs into the nested leaf.
	reference<const Node> leaf_ref(node);
	while (leaf_ref.get().HasMetadata()) {
		auto &leaf = Node::Ref<const Leaf>(art, leaf_ref, NType::LEAF);
		for (idx_t i = 0; i < leaf.count; i++) {
			auto row_id = ARTKey::CreateARTKey<row_t>(allocator, leaf.row_ids[i]);
			art.Insert(root, row_id, 0, row_id, true);
		}
		leaf_ref = leaf.ptr;
	}

	root.SetGate();
	Node::Free(art, node);
	node = root;
}

void Leaf::TransformToDeprecated(ART &art, Node &node) {
	D_ASSERT(node.IsGate() || node.GetType() == NType::LEAF);

	// Early-out, if we never transformed this leaf.
	if (!node.IsGate()) {
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

	// Create the deprecated leaf.
	idx_t remaining_count = row_ids.size();
	idx_t copy_count = 0;
	reference<Node> ref_node(node);
	while (remaining_count) {
		ref_node.get() = Node::GetAllocator(art, NType::LEAF).New();
		ref_node.get().SetMetadata(static_cast<uint8_t>(NType::LEAF));

		auto &leaf = Node::RefMutable<Leaf>(art, ref_node, NType::LEAF);
		leaf.count = UnsafeNumericCast<uint8_t>(MinValue((idx_t)Node::LEAF_SIZE, remaining_count));

		for (idx_t i = 0; i < leaf.count; i++) {
			leaf.row_ids[i] = row_ids[copy_count + i];
		}

		copy_count += leaf.count;
		remaining_count -= leaf.count;

		ref_node = leaf.ptr;
		leaf.ptr.Clear();
	}
}

//===--------------------------------------------------------------------===//
// Debug-only functions.
//===--------------------------------------------------------------------===//

bool Leaf::ContainsRowId(ART &art, const Node &node, const ARTKey &row_id) {
	D_ASSERT(node.HasMetadata());

	if (node.GetType() == NType::LEAF_INLINED) {
		return node.GetRowId() == row_id.GetRowID();
	}

	// Note: This is a DEBUG function. We only call this after ART::Insert, ART::Delete,
	// and ART::ConstructFromSorted. Thus, it can never have deprecated storage.
	D_ASSERT(node.IsGate());
	return art.Lookup(node, row_id, 0) != nullptr;
}

//===--------------------------------------------------------------------===//
// Deprecated code paths.
//===--------------------------------------------------------------------===//

void Leaf::DeprecatedFree(ART &art, Node &node) {
	D_ASSERT(node.GetType() == NType::LEAF);

	Node next_node;
	while (node.HasMetadata()) {
		next_node = Node::RefMutable<Leaf>(art, node, NType::LEAF).ptr;
		Node::GetAllocator(art, NType::LEAF).Free(node);
		node = next_node;
	}
	node.Clear();
}

bool Leaf::DeprecatedGetRowIds(ART &art, const Node &node, unsafe_vector<row_t> &row_ids, idx_t max_count) {
	D_ASSERT(node.GetType() == NType::LEAF);

	// Push back all row IDs of this leaf.
	reference<const Node> last_leaf_ref(node);
	while (last_leaf_ref.get().HasMetadata()) {
		auto &leaf = Node::Ref<const Leaf>(art, last_leaf_ref, NType::LEAF);

		// Never return more than max_count row IDs.
		if (row_ids.size() + leaf.count > max_count) {
			return false;
		}
		for (idx_t i = 0; i < leaf.count; i++) {
			row_ids.push_back(leaf.row_ids[i]);
		}
		last_leaf_ref = leaf.ptr;
	}
	return true;
}

void Leaf::DeprecatedVacuum(ART &art, Node &node) {
	D_ASSERT(node.HasMetadata());
	D_ASSERT(node.GetType() == NType::LEAF);

	auto &allocator = Node::GetAllocator(art, NType::LEAF);
	reference<Node> node_ref(node);
	while (node_ref.get().HasMetadata()) {
		if (allocator.NeedsVacuum(node_ref)) {
			node_ref.get() = allocator.VacuumPointer(node_ref);
			node_ref.get().SetMetadata(static_cast<uint8_t>(NType::LEAF));
		}
		auto &leaf = Node::RefMutable<Leaf>(art, node_ref, NType::LEAF);
		node_ref = leaf.ptr;
	}
}

string Leaf::DeprecatedVerifyAndToString(ART &art, const Node &node, const bool only_verify) {
	D_ASSERT(node.GetType() == NType::LEAF);

	string str = "";
	reference<const Node> node_ref(node);

	while (node_ref.get().HasMetadata()) {
		auto &leaf = Node::Ref<const Leaf>(art, node_ref, NType::LEAF);
		D_ASSERT(leaf.count <= Node::LEAF_SIZE);

		str += "Leaf [count: " + to_string(leaf.count) + ", row IDs: ";
		for (idx_t i = 0; i < leaf.count; i++) {
			str += to_string(leaf.row_ids[i]) + "-";
		}
		str += "] ";
		node_ref = leaf.ptr;
	}

	return only_verify ? "" : str;
}

} // namespace duckdb
