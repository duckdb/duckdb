#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

void Leaf::DeprecatedNew(ART &art, reference<Node> &node, const vector<ARTKey> &row_ids, const idx_t start,
                         const idx_t count) {
	D_ASSERT(count > 1);

	idx_t copy_count = 0;
	idx_t remaining_count = count;
	while (remaining_count) {
		node.get() = Node::GetAllocator(art, NType::LEAF).New();
		node.get().SetMetadata(static_cast<uint8_t>(NType::LEAF));

		auto &leaf = Node::RefMutable<Leaf>(art, node, NType::LEAF);
		leaf.count = UnsafeNumericCast<uint8_t>(MinValue((idx_t)Node::LEAF_SIZE, remaining_count));

		for (idx_t i = 0; i < leaf.count; i++) {
			leaf.row_ids[i] = row_ids[start + copy_count + i].GetRowID();
		}

		copy_count += leaf.count;
		remaining_count -= leaf.count;

		node = leaf.ptr;
		leaf.ptr.Clear();
	}
}

void Leaf::DeprecatedFree(ART &art, Node &node) {

	Node current_node = node;
	Node next_node;
	while (current_node.HasMetadata()) {
		next_node = Node::RefMutable<Leaf>(art, current_node, NType::LEAF).ptr;
		Node::GetAllocator(art, NType::LEAF).Free(current_node);
		current_node = next_node;
	}

	node.Clear();
}

void Leaf::DeprecatedInitializeMerge(ART &art, Node &node, const ARTFlags &flags) {

	auto merge_buffer_count = flags.merge_buffer_counts[static_cast<uint8_t>(NType::LEAF) - 1];

	Node next_node = node;
	node.IncreaseBufferId(merge_buffer_count);

	while (next_node.HasMetadata()) {
		auto &leaf = Node::RefMutable<Leaf>(art, next_node, NType::LEAF);
		next_node = leaf.ptr;
		if (leaf.ptr.HasMetadata()) {
			leaf.ptr.IncreaseBufferId(merge_buffer_count);
		}
	}
}

void Leaf::DeprecatedMerge(ART &art, Node &l_node, Node &r_node) {

	// copy inlined row ID of r_node
	if (r_node.GetType() == NType::LEAF_INLINED) {
		DeprecatedInsert(art, l_node, r_node.GetRowId());
		r_node.Clear();
		return;
	}

	// l_node has an inlined row ID, swap and insert
	if (l_node.GetType() == NType::LEAF_INLINED) {
		auto row_id = l_node.GetRowId();
		l_node = r_node;
		DeprecatedInsert(art, l_node, row_id);
		r_node.Clear();
		return;
	}

	D_ASSERT(l_node.GetType() != NType::LEAF_INLINED);
	D_ASSERT(r_node.GetType() != NType::LEAF_INLINED);

	reference<Node> l_node_ref(l_node);
	reference<Leaf> l_leaf = Node::RefMutable<Leaf>(art, l_node_ref, NType::LEAF);

	// find a non-full node
	while (l_leaf.get().count == Node::LEAF_SIZE) {
		l_node_ref = l_leaf.get().ptr;

		// the last leaf is full
		if (!l_leaf.get().ptr.HasMetadata()) {
			break;
		}
		l_leaf = Node::RefMutable<Leaf>(art, l_node_ref, NType::LEAF);
	}

	// store the last leaf and then append r_node
	auto last_leaf_node = l_node_ref.get();
	l_node_ref.get() = r_node;
	r_node.Clear();

	// append the remaining row IDs of the last leaf node
	if (last_leaf_node.HasMetadata()) {
		// find the tail
		l_leaf = Node::RefMutable<Leaf>(art, l_node_ref, NType::LEAF);
		while (l_leaf.get().ptr.HasMetadata()) {
			l_leaf = Node::RefMutable<Leaf>(art, l_leaf.get().ptr, NType::LEAF);
		}
		// append the row IDs
		auto &last_leaf = Node::RefMutable<Leaf>(art, last_leaf_node, NType::LEAF);
		for (idx_t i = 0; i < last_leaf.count; i++) {
			l_leaf = l_leaf.get().DeprecatedAppend(art, last_leaf.row_ids[i]);
		}
		Node::GetAllocator(art, NType::LEAF).Free(last_leaf_node);
	}
}

void Leaf::DeprecatedInsert(ART &art, Node &node, const row_t row_id) {
	if (node.GetType() == NType::LEAF_INLINED) {
		DeprecatedMoveInlinedToLeaf(art, node);
		DeprecatedInsert(art, node, row_id);
		return;
	}

	// append to the tail
	reference<Leaf> leaf = Node::RefMutable<Leaf>(art, node, NType::LEAF);
	while (leaf.get().ptr.HasMetadata()) {
		leaf = Node::RefMutable<Leaf>(art, leaf.get().ptr, NType::LEAF);
	}
	leaf.get().DeprecatedAppend(art, row_id);
}

bool Leaf::DeprecatedRemove(ART &art, reference<Node> &node, const row_t row_id) {
	reference<Leaf> leaf = Node::RefMutable<Leaf>(art, node, NType::LEAF);

	// inline the remaining row ID
	if (leaf.get().count == 2) {
		if (leaf.get().row_ids[0] == row_id || leaf.get().row_ids[1] == row_id) {
			auto remaining_row_id = leaf.get().row_ids[0] == row_id ? leaf.get().row_ids[1] : leaf.get().row_ids[0];
			Node::Free(art, node);
			New(node, remaining_row_id);
		}
		return false;
	}

	// get the last row ID (the order within a leaf does not matter)
	// because we want to overwrite the row ID to remove with that one

	// go to the tail and keep track of the previous leaf node
	reference<Leaf> prev_leaf(leaf);
	while (leaf.get().ptr.HasMetadata()) {
		prev_leaf = leaf;
		leaf = Node::RefMutable<Leaf>(art, leaf.get().ptr, NType::LEAF);
	}

	auto last_idx = leaf.get().count;
	auto last_row_id = leaf.get().row_ids[last_idx - 1];

	// only one row ID in this leaf segment, free it
	if (leaf.get().count == 1) {
		Node::Free(art, prev_leaf.get().ptr);
		if (last_row_id == row_id) {
			return false;
		}
	} else {
		leaf.get().count--;
	}

	// find the row ID and copy the last row ID to that position
	while (node.get().HasMetadata()) {
		leaf = Node::RefMutable<Leaf>(art, node, NType::LEAF);
		for (idx_t i = 0; i < leaf.get().count; i++) {
			if (leaf.get().row_ids[i] == row_id) {
				leaf.get().row_ids[i] = last_row_id;
				return false;
			}
		}
		node = leaf.get().ptr;
	}
	return false;
}

idx_t Leaf::DeprecatedTotalCount(ART &art, const Node &node) {

	D_ASSERT(node.HasMetadata());
	if (node.GetType() == NType::LEAF_INLINED) {
		return 1;
	}

	idx_t count = 0;
	reference<const Node> node_ref(node);
	while (node_ref.get().HasMetadata()) {
		auto &leaf = Node::Ref<const Leaf>(art, node_ref, NType::LEAF);
		count += leaf.count;
		node_ref = leaf.ptr;
	}
	return count;
}

bool Leaf::DeprecatedGetRowIds(ART &art, const Node &node, vector<row_t> &row_ids, idx_t max_count) {
	// adding more elements would exceed the maximum count
	D_ASSERT(node.HasMetadata());
	if (row_ids.size() + DeprecatedTotalCount(art, node) > max_count) {
		return false;
	}

	// push back all the row IDs of this leaf
	reference<const Node> last_leaf_ref(node);
	while (last_leaf_ref.get().HasMetadata()) {
		auto &leaf = Node::Ref<const Leaf>(art, last_leaf_ref, NType::LEAF);
		for (idx_t i = 0; i < leaf.count; i++) {
			row_ids.push_back(leaf.row_ids[i]);
		}
		last_leaf_ref = leaf.ptr;
	}
	return true;
}

bool Leaf::DeprecatedContainsRowId(ART &art, const Node &node, const row_t row_id) {
	reference<const Node> ref_node(node);
	while (ref_node.get().HasMetadata()) {
		auto &leaf = Node::Ref<const Leaf>(art, ref_node, NType::LEAF);
		for (idx_t i = 0; i < leaf.count; i++) {
			if (leaf.row_ids[i] == row_id) {
				return true;
			}
		}
		ref_node = leaf.ptr;
	}
	return false;
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

void Leaf::DeprecatedMoveInlinedToLeaf(ART &art, Node &node) {

	D_ASSERT(node.GetType() == NType::LEAF_INLINED);
	auto row_id = node.GetRowId();
	auto &leaf = New(art, node);

	leaf.count = 1;
	leaf.row_ids[0] = row_id;
}

Leaf &Leaf::DeprecatedAppend(ART &art, const row_t row_id) {

	reference<Leaf> leaf(*this);

	// we need a new leaf node
	if (leaf.get().count == Node::LEAF_SIZE) {
		leaf = New(art, leaf.get().ptr);
	}

	leaf.get().row_ids[leaf.get().count] = row_id;
	leaf.get().count++;
	return leaf.get();
}

} // namespace duckdb
