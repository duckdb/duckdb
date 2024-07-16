#include "duckdb/execution/index/art/leaf.hpp"

#include "duckdb/common/types.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/iterator.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/execution/index/art/prefix.hpp"

namespace duckdb {

//! Deserialize a deprecated leaf into a nested leaf.
//! Serialize a nested leaf into a deprecated leaf.

ARTKey KeyFromRowId(AttachedDatabase &db, const row_t row_id) {
	ArenaAllocator arena_allocator(Allocator::Get(db));
	LogicalType row_id_type(LogicalType::ROW_TYPE);
	auto value = Value::BIGINT(row_id);
	return ARTKey::CreateKey(arena_allocator, row_id_type.InternalType(), value);
}

Node &UnnestMutable(ART &art, Node &node) {
	D_ASSERT(node.GetType() == NType::LEAF);
	auto &leaf = Node::RefMutable<Leaf>(art, node, NType::LEAF);
	D_ASSERT(leaf.count == 0);
	D_ASSERT(leaf.ptr.HasMetadata());
	return leaf.ptr;
}

const Node &Unnest(ART &art, const Node &node) {
	D_ASSERT(node.GetType() == NType::LEAF);
	auto &leaf = Node::Ref<const Leaf>(art, node, NType::LEAF);
	D_ASSERT(leaf.count == 0);
	D_ASSERT(leaf.ptr.HasMetadata());
	return leaf.ptr;
}

void Leaf::New(Node &node, const row_t row_id) {
	D_ASSERT(row_id < MAX_ROW_ID_LOCAL);

	node.Clear();
	node.SetMetadata(static_cast<uint8_t>(NType::LEAF_INLINED));
	node.SetRowId(row_id);
}

Leaf &Leaf::New(ART &art, Node &node) {
	node = Node::GetAllocator(art, NType::LEAF).New();
	node.SetMetadata(static_cast<uint8_t>(NType::LEAF));
	auto &leaf = Node::RefMutable<Leaf>(art, node, NType::LEAF);

	leaf.count = 0;
	leaf.ptr.Clear();
	return leaf;
}

void Leaf::New(ART &art, reference<Node> &node, const vector<ARTKey> &row_ids, const idx_t start, const idx_t count) {
	D_ASSERT(count > 1);

	auto &leaf = Leaf::New(art, node);
	ARTKeySection section(start, start + count - 1, 0, 0);
	art.Construct(row_ids, row_ids, leaf.ptr, section);
}

void Leaf::Free(ART &art, Node &node) {
	D_ASSERT(node.HasMetadata());

	auto &inner_art = UnnestMutable(art, node);
	Node::Free(art, inner_art);
	Node::GetAllocator(art, NType::LEAF).Free(node);
	node.Clear();
}

void Leaf::InitializeMerge(ART &art, Node &node, const ARTFlags &flags) {
	D_ASSERT(node.HasMetadata());

	auto &inner_art = UnnestMutable(art, node);
	auto merge_buffer_count = flags.merge_buffer_counts[static_cast<uint8_t>(NType::LEAF) - 1];
	node.IncreaseBufferId(merge_buffer_count);
	inner_art.InitializeMerge(art, flags);
}

void MergeInlined(ART &art, Node &l_node, Node &r_node) {

	// Create an ARTKey from the row ID.
	ArenaAllocator arena_allocator(Allocator::Get(art.db));
	LogicalType row_id_type(LogicalType::ROW_TYPE);
	auto value = Value::BIGINT(r_node.GetRowId());
	auto key = ARTKey::CreateKey(arena_allocator, row_id_type.InternalType(), value);

	// Insert the ARTKey.
	Leaf::Insert(art, l_node, key);
	r_node.Clear();
}

void Leaf::Merge(ART &art, Node &l_node, Node &r_node) {
	D_ASSERT(l_node.HasMetadata());
	D_ASSERT(r_node.HasMetadata());

	// Copy the inlined row ID of r_node into l_node.
	if (r_node.GetType() == NType::LEAF_INLINED) {
		return MergeInlined(art, l_node, r_node);
	}

	// l_node has an inlined row ID, swap and insert.
	if (l_node.GetType() == NType::LEAF_INLINED) {
		auto temp_node = l_node;
		l_node = r_node;
		MergeInlined(art, l_node, temp_node);
		return r_node.Clear();
	}

	D_ASSERT(l_node.GetType() != NType::LEAF_INLINED);
	D_ASSERT(r_node.GetType() != NType::LEAF_INLINED);

	auto &inner_l_art = UnnestMutable(art, l_node);
	auto &inner_r_art = UnnestMutable(art, r_node);
	inner_l_art.Merge(art, inner_r_art);
}

void InsertIntoInlined(ART &art, Node &node, const ARTKey &row_id) {

	// Insert a NType::LEAF guard with a count of 0.
	auto inlined_row_id = node.GetRowId();
	auto &leaf = Leaf::New(art, node);

	// Insert both row IDs into the nested ART.
	auto inlined_row_id_key = KeyFromRowId(art.db, inlined_row_id);
	art.Insert(leaf.ptr, inlined_row_id_key, 0, inlined_row_id_key);
	art.Insert(leaf.ptr, row_id, 0, row_id);
}

void Leaf::Insert(ART &art, Node &node, const ARTKey &row_id) {
	D_ASSERT(node.HasMetadata());

	if (node.GetType() == NType::LEAF_INLINED) {
		return InsertIntoInlined(art, node, row_id);
	}

	D_ASSERT(node.GetType() == NType::LEAF);
	auto &leaf = Node::RefMutable<Leaf>(art, node, NType::LEAF);
	if (leaf.count != 0) {
		// This is a deprecated leaf. We transform it.
		// TODO: transform.
		leaf.count = 0;
	}

	art.Insert(leaf.ptr, row_id, 0, row_id);
}

bool Leaf::Remove(ART &art, reference<Node> &node, const ARTKey &row_id) {
	D_ASSERT(node.get().HasMetadata());

	if (node.get().GetType() == NType::LEAF_INLINED) {
		return node.get().GetRowId() == row_id.GetRowID();
	}

	D_ASSERT(node.get().GetType() == NType::LEAF);
	auto &leaf = Node::RefMutable<Leaf>(art, node, NType::LEAF);
	if (leaf.count != 0) {
		// This is a deprecated leaf. We transform it.
		// TODO: transform.
		leaf.count = 0;
	}

	// Erase the row ID.
	art.Erase(leaf.ptr, row_id, 0, row_id);

	// Traverse the prefix.
	reference<const Node> prefix_node(leaf.ptr);
	while (prefix_node.get().GetType() == NType::PREFIX) {
		auto &prefix = Node::Ref<const Prefix>(art, prefix_node, NType::PREFIX);
		prefix_node = prefix.ptr;
		D_ASSERT(prefix.ptr.HasMetadata());
	}

	// If the inner ART is an inlined leaf, we inline it.
	if (prefix_node.get().GetType() == NType::LEAF_INLINED) {
		auto remaining_row_id = prefix_node.get().GetRowId();
		Node::Free(art, node);
		New(node, remaining_row_id);
	}
	return false;
}

bool Leaf::GetRowIds(ART &art, const Node &node, vector<row_t> &row_ids, idx_t max_count) {
	D_ASSERT(node.HasMetadata());

	if (node.GetType() == NType::LEAF_INLINED) {
		if (row_ids.size() + 1 > max_count) {
			return false;
		}
		row_ids.push_back(node.GetRowId());
		return true;
	}

	D_ASSERT(node.GetType() == NType::LEAF);
	auto &leaf = Node::RefMutable<Leaf>(art, node, NType::LEAF);
	if (leaf.count != 0) {
		// This is a deprecated leaf and GetRowIds is a const operation.
		// Thus, we cannot transform the leaf and enter a deprecated code path.
		return DeprecatedGetRowIds(art, node, row_ids, max_count);
	}

	auto &inner_art = Unnest(art, node);
	Iterator it;
	if (!it.art) {
		it.art = art;
		it.FindMinimum(inner_art);
	}

	ARTKey empty_key = ARTKey();
	return it.Scan(empty_key, max_count, row_ids, false);
}

void Leaf::Vacuum(ART &art, Node &node, const ARTFlags &flags) {
	D_ASSERT(node.HasMetadata());
	D_ASSERT(node.GetType() == NType::LEAF);

	if (!flags.vacuum_flags[static_cast<uint8_t>(NType::LEAF) - 1]) {
		return;
	}

	auto &leaf = Node::RefMutable<Leaf>(art, node, NType::LEAF);
	if (leaf.count != 0) {
		// This is a deprecated leaf. We could transform the leaf, and might decide
		// to do so in the future. For now, we opt against it. By transforming, we potentially
		// create new buffers. Increasing memory during a vacuum operation is not ideal.
		// Thus, we enter a deprecated code path.
		return DeprecatedVacuum(art, node);
	}

	leaf.ptr.Vacuum(art, flags);

	auto &allocator = Node::GetAllocator(art, NType::LEAF);
	if (allocator.NeedsVacuum(node)) {
		node = allocator.VacuumPointer(node);
		node.SetMetadata(static_cast<uint8_t>(NType::LEAF));
	}
}

//===--------------------------------------------------------------------===//
// Debug-only functions.
//===--------------------------------------------------------------------===//

string Leaf::VerifyAndToString(ART &art, const Node &node, const bool only_verify) {
	if (node.GetType() == NType::LEAF_INLINED) {
		return only_verify ? "" : "Leaf Inlined [count: 1, row ID: " + to_string(node.GetRowId()) + "]";
	}

	D_ASSERT(node.GetType() == NType::LEAF);
	auto &leaf = Node::RefMutable<Leaf>(art, node, NType::LEAF);
	if (leaf.count != 0) {
		// Note: this is a DEBUG function and a const operation.
		// Thus, we cannot transform the leaf and enter a deprecated code path.
		return DeprecatedVerifyAndToString(art, node, only_verify);
	}

	auto &inner_art = Unnest(art, node);
	auto inner_str = inner_art.VerifyAndToString(art, only_verify);
	return only_verify ? "" : "Leaf [" + inner_str + "]";
}

bool Leaf::ContainsRowId(ART &art, const Node &node, const ARTKey &row_id) {
	D_ASSERT(node.HasMetadata());

	if (node.GetType() == NType::LEAF_INLINED) {
		return node.GetRowId() == row_id.GetRowID();
	}

	// Note: this is a DEBUG function. We only call this after ART::Insert, ART::Delete,
	// and ART::ConstructFromSorted. Thus, it can never have deprecated storage.
	auto &inner_art = Unnest(art, node);
	auto leaf = art.Lookup(inner_art, row_id, 0);
	return leaf != nullptr;
}

//===--------------------------------------------------------------------===//
// Deprecated code paths.
//===--------------------------------------------------------------------===//

bool Leaf::DeprecatedGetRowIds(ART &art, const Node &node, vector<row_t> &row_ids, idx_t max_count) {

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
