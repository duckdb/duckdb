#include "duckdb/execution/index/art/leaf.hpp"

#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/execution/index/art/iterator.hpp"

namespace duckdb {

void Leaf::New(Node &node, const row_t row_id) {
	// We directly inline this row ID into the node pointer.
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
	if (!art.nested_leaves) {
		return DeprecatedNew(art, node, row_ids, start, count);
	}

	auto &leaf = Leaf::New(art, node);
	ARTKeySection section(start, count - 1, 0, 0);
	art.Construct(row_ids, row_ids, leaf.ptr, section);
}

void Leaf::Free(ART &art, Node &node) {
	if (!art.nested_leaves) {
		return DeprecatedFree(art, node);
	}

	auto &inner_art = UnnestMutable(art, node);
	Node::Free(art, inner_art);
	Node::GetAllocator(art, NType::LEAF).Free(node);
	node.Clear();
}

void Leaf::InitializeMerge(ART &art, Node &node, const ARTFlags &flags) {
	if (!art.nested_leaves) {
		return DeprecatedInitializeMerge(art, node, flags);
	}

	auto &inner_art = UnnestMutable(art, node);
	auto merge_buffer_count = flags.merge_buffer_counts[static_cast<uint8_t>(NType::LEAF) - 1];
	node.IncreaseBufferId(merge_buffer_count);

	inner_art.InitializeMerge(art, flags);
}

void Leaf::Merge(ART &art, Node &l_node, Node &r_node) {
	D_ASSERT(l_node.HasMetadata() && r_node.HasMetadata());
	if (!art.nested_leaves) {
		return DeprecatedMerge(art, l_node, r_node);
	}

	// Copy the inlined row ID of r_node into l_node.
	if (r_node.GetType() == NType::LEAF_INLINED) {
		return MergeInlinedIntoLeaf(art, l_node, r_node);
	}

	// l_node has an inlined row ID, swap and insert.
	if (l_node.GetType() == NType::LEAF_INLINED) {
		auto temp_node = l_node;
		l_node = r_node;
		MergeInlinedIntoLeaf(art, l_node, temp_node);
		r_node.Clear();
		return;
	}

	D_ASSERT(l_node.GetType() != NType::LEAF_INLINED);
	D_ASSERT(r_node.GetType() != NType::LEAF_INLINED);

	auto &inner_l_art = UnnestMutable(art, l_node);
	auto &inner_r_art = UnnestMutable(art, r_node);
	inner_l_art.Merge(art, inner_r_art);
}

void Leaf::Insert(ART &art, Node &node, const ARTKey &row_id) {
	D_ASSERT(node.HasMetadata());
	if (!art.nested_leaves) {
		return DeprecatedInsert(art, node, row_id.GetRowID());
	}

	if (node.GetType() == NType::LEAF_INLINED) {
		// Insert a NType::LEAF guard with a count of 0.
		auto inlined_node = node;
		auto &new_leaf = Leaf::New(art, node);
		new_leaf.ptr = inlined_node;
	}

	auto &inner_art = UnnestMutable(art, node);
	art.Insert(inner_art, row_id, 0, row_id);
}

bool Leaf::Remove(ART &art, reference<Node> &node, const ARTKey &row_id) {
	D_ASSERT(node.get().HasMetadata());
	if (node.get().GetType() == NType::LEAF_INLINED) {
		return node.get().GetRowId() == row_id.GetRowID();
	}

	if (!art.nested_leaves) {
		return DeprecatedRemove(art, node, row_id.GetRowID());
	}

	auto &inner_art = UnnestMutable(art, node.get());
	art.Erase(inner_art, row_id, 0, row_id);

	// If the inner ART is an inlined leaf, we inline it.
	if (inner_art.GetType() == NType::LEAF_INLINED) {
		auto remaining_row_id = inner_art.GetRowId();
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

	if (!art.nested_leaves) {
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

bool Leaf::ContainsRowId(ART &art, const Node &node, const ARTKey &row_id) {
	D_ASSERT(node.HasMetadata());
	if (node.GetType() == NType::LEAF_INLINED) {
		return node.GetRowId() == row_id.GetRowID();
	}

	if (!art.nested_leaves) {
		return DeprecatedContainsRowId(art, node, row_id.GetRowID());
	}

	auto &inner_art = Unnest(art, node);
	auto leaf = art.Lookup(inner_art, row_id, 0);
	return leaf != nullptr;
}

string Leaf::VerifyAndToString(ART &art, const Node &node, const bool only_verify) {
	if (node.GetType() == NType::LEAF_INLINED) {
		return only_verify ? "" : "Leaf [count: 1, row ID: " + to_string(node.GetRowId()) + "]";
	}

	if (!art.nested_leaves) {
		return DeprecatedVerifyAndToString(art, node, only_verify);
	}

	auto &inner_art = Unnest(art, node);
	auto inner_str = inner_art.VerifyAndToString(art, only_verify);
	return only_verify ? "" : "Leaf [" + inner_str + "]";
}

void Leaf::Vacuum(ART &art, Node &node, const ARTFlags &flags) {
	if (!flags.vacuum_flags[static_cast<uint8_t>(NType::LEAF) - 1]) {
		return;
	}

	if (!art.nested_leaves) {
		return DeprecatedVacuum(art, node);
	}

	auto &inner_art = UnnestMutable(art, node);
	inner_art.Vacuum(art, flags);

	auto &allocator = Node::GetAllocator(art, NType::LEAF);
	if (allocator.NeedsVacuum(node)) {
		node = allocator.VacuumPointer(node);
		node.SetMetadata(static_cast<uint8_t>(NType::LEAF));
	}
}

Node &Leaf::UnnestMutable(ART &art, Node &node) {
	D_ASSERT(node.GetType() == NType::LEAF);
	auto &leaf = Node::RefMutable<Leaf>(art, node, NType::LEAF);
	D_ASSERT(leaf.count == 0);
	D_ASSERT(leaf.ptr.HasMetadata());
	return leaf.ptr;
}

const Node &Leaf::Unnest(ART &art, const Node &node) {
	D_ASSERT(node.GetType() == NType::LEAF);
	auto &leaf = Node::Ref<const Leaf>(art, node, NType::LEAF);
	D_ASSERT(leaf.count == 0);
	D_ASSERT(leaf.ptr.HasMetadata());
	return leaf.ptr;
}

void Leaf::MergeInlinedIntoLeaf(ART &art, Node &l_node, Node &r_node) {

	// Create an ARTKey from the row ID.
	ArenaAllocator arena_allocator(Allocator::Get(art.db));
	LogicalType row_id_type(LogicalType::ROW_TYPE);
	auto value = Value::UBIGINT(NumericCast<idx_t>(r_node.GetRowId()));
	auto key = ARTKey::CreateKey(arena_allocator, row_id_type.InternalType(), value);

	// Insert the ARTKey.
	Insert(art, l_node, key);
	r_node.Clear();
}

} // namespace duckdb
