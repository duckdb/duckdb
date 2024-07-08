#include "duckdb/execution/index/art/leaf.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/common/numeric_utils.hpp"

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

void Leaf::Insert(ART &art, Node &node, const ARTKey &row_id_key) {
	D_ASSERT(node.HasMetadata());
	if (art.deprecated) {
		return DeprecatedInsert(art, node, row_id_key.GetRowID());
	}

	if (node.GetType() == NType::LEAF_INLINED) {
		// Insert a NType::LEAF guard with a count of 0.
		auto inlined_node = node;
		auto &new_leaf = Leaf::New(art, node);
		new_leaf.ptr = inlined_node;
	}

	auto &inner_art = UnnestMutable(art, node);
	art.Insert(inner_art, row_id_key, 0, row_id_key);
}

bool Leaf::Remove(ART &art, reference<Node> &node, const ARTKey &row_id_key) {
	D_ASSERT(node.get().HasMetadata());
	if (node.get().GetType() == NType::LEAF_INLINED) {
		return node.get().GetRowId() == row_id_key.GetRowID();
	}

	if (art.deprecated) {
		return DeprecatedRemove(art, node, row_id_key.GetRowID());
	}

	auto &inner_art = UnnestMutable(art, node.get());
	art.Erase(inner_art, row_id_key, 0, row_id_key);

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

	if (art.deprecated) {
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

bool Leaf::ContainsRowId(ART &art, const Node &node, const ARTKey &row_id_key) {
	D_ASSERT(node.HasMetadata());
	if (node.GetType() == NType::LEAF_INLINED) {
		return node.GetRowId() == row_id_key.GetRowID();
	}

	if (art.deprecated) {
		return DeprecatedContainsRowId(art, node, row_id_key.GetRowID());
	}

	auto &inner_art = Unnest(art, node);
	auto leaf = art.Lookup(inner_art, row_id_key, 0);
	return leaf != nullptr;
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

} // namespace duckdb
