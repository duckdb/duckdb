#include "duckdb/execution/index/art/leaf.hpp"

#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

void Leaf::New(Node &node, const row_t row_id) {

	// We directly inline this row ID into the node pointer.
	D_ASSERT(row_id < MAX_ROW_ID_LOCAL);
	node.Clear();
	node.SetMetadata(static_cast<uint8_t>(NType::LEAF_INLINED));
	node.SetRowId(row_id);
}

void Leaf::New(ART &art, reference<Node> &node, const row_t *row_ids, idx_t count) {

	D_ASSERT(count > 1);

	// We append to the tail, possibly leaving the tail not entirely filled.
	idx_t copy_count = 0;
	while (count) {
		node.get() = Node::GetAllocator(art, NType::LEAF).New();
		node.get().SetMetadata(static_cast<uint8_t>(NType::LEAF));

		auto &leaf = Node::RefMutable<Leaf>(art, node, NType::LEAF);
		leaf.count = UnsafeNumericCast<uint8_t>(MinValue((idx_t)Node::LEAF_SIZE, count));
		for (idx_t i = 0; i < leaf.count; i++) {
			leaf.row_ids[i] = row_ids[copy_count + i];
		}

		copy_count += leaf.count;
		count -= leaf.count;

		node = leaf.ptr;
		leaf.ptr.Clear();
	}
}

Leaf &Leaf::New(ART &art, Node &node) {
	node = Node::GetAllocator(art, NType::LEAF).New();
	node.SetMetadata(static_cast<uint8_t>(NType::LEAF));
	auto &leaf = Node::RefMutable<Leaf>(art, node, NType::LEAF);

	leaf.count = 0;
	leaf.ptr.Clear();
	return leaf;
}

void Leaf::FreeChain(ART &art, Node &node) {
	Node next_node;
	while (node.HasMetadata()) {
		next_node = Node::RefMutable<Leaf>(art, node, NType::LEAF).ptr;
		Node::GetAllocator(art, NType::LEAF).Free(node);
		node = next_node;
	}
	node.Clear();
}

void Leaf::Free(ART &art, Node &node) {
	auto next_node = Node::RefMutable<Leaf>(art, node, NType::LEAF).ptr;
	Node::GetAllocator(art, NType::LEAF).Free(node);
	node = next_node;
}

void Leaf::InitializeMerge(ART &art, Node &node, const ARTFlags &flags) {

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

void Leaf::Merge(ART &art, Node &l_node, Node &r_node) {

	D_ASSERT(l_node.HasMetadata() && r_node.HasMetadata());

	// Copy the inlined row ID of r_node.
	if (r_node.GetType() == NType::LEAF_INLINED) {
		Insert(art, l_node, r_node.GetRowId());
		r_node.Clear();
		return;
	}

	// l_node has an inlined row ID, swap and insert.
	if (l_node.GetType() == NType::LEAF_INLINED) {
		auto row_id = l_node.GetRowId();
		l_node = r_node;
		Insert(art, l_node, row_id);
		r_node.Clear();
		return;
	}

	D_ASSERT(l_node.GetType() != NType::LEAF_INLINED);
	D_ASSERT(r_node.GetType() != NType::LEAF_INLINED);

	// Append r_node to l_node.
	reference<Leaf> leaf = Node::RefMutable<Leaf>(art, l_node, NType::LEAF);
	while (leaf.get().ptr.HasMetadata()) {
		leaf = Node::RefMutable<Leaf>(art, leaf.get().ptr, NType::LEAF);
	}

	leaf.get().ptr = r_node;
	r_node.Clear();
}

void Leaf::Insert(ART &art, Node &node, const row_t row_id) {

	D_ASSERT(node.HasMetadata());

	if (node.GetType() == NType::LEAF_INLINED) {
		MoveInlinedToLeaf(art, node);
		Insert(art, node, row_id);
		return;
	}

	// Append to the head.
	reference<Leaf> leaf = Node::RefMutable<Leaf>(art, node, NType::LEAF);

	// Add a new leaf to the front.
	if (leaf.get().count == Node::LEAF_SIZE) {
		auto head = node;
		leaf = New(art, node);
		leaf.get().ptr = head;
	}

	leaf.get().row_ids[leaf.get().count] = row_id;
	leaf.get().count++;
}

bool RemoveInternal(Leaf &leaf, const row_t row_id) {
	for (idx_t i = 0; i < leaf.count; i++) {
		if (leaf.row_ids[i] != row_id) {
			continue;
		}

		// Shift the remaining row IDs and decrement the count.
		for (idx_t j = i + 1; j < leaf.count; j++) {
			leaf.row_ids[j - 1] = leaf.row_ids[j];
		}
		leaf.count--;
		return true;
	}
	return false;
}

bool Leaf::Remove(ART &art, reference<Node> &node, const row_t row_id) {

	D_ASSERT(node.get().HasMetadata());

	if (node.get().GetType() == NType::LEAF_INLINED) {
		if (node.get().GetRowId() == row_id) {
			return true;
		}
		return false;
	}

	// Remove the row ID and possibly the leaf.
	reference<Node> ref_node(node);
	while (ref_node.get().HasMetadata()) {
		auto &leaf = Node::RefMutable<Leaf>(art, ref_node, NType::LEAF);
		if (RemoveInternal(leaf, row_id)) {
			if (leaf.count == 0) {
				Leaf::Free(art, ref_node);
			}
			break;
		}
		ref_node = leaf.ptr;
	}

	// Check for inline.
	auto &leaf = Node::RefMutable<Leaf>(art, node, NType::LEAF);
	if (!leaf.ptr.HasMetadata()) {
		if (leaf.count == 1) {
			auto remaining_row_id = leaf.row_ids[0];
			Leaf::Free(art, node);
			New(node, remaining_row_id);
		}
	}

	// FIXME: To prevent fragmentation, we can swap the first non-full leave to the front.
	return false;
}

idx_t Leaf::TotalCount(ART &art, const Node &node) {

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

bool Leaf::GetRowIds(ART &art, const Node &node, vector<row_t> &result_ids, idx_t max_count) {

	// adding more elements would exceed the maximum count
	D_ASSERT(node.HasMetadata());
	if (result_ids.size() + TotalCount(art, node) > max_count) {
		return false;
	}

	if (node.GetType() == NType::LEAF_INLINED) {
		// push back the inlined row ID of this leaf
		result_ids.push_back(node.GetRowId());

	} else {
		// push back all the row IDs of this leaf
		reference<const Node> last_leaf_ref(node);
		while (last_leaf_ref.get().HasMetadata()) {
			auto &leaf = Node::Ref<const Leaf>(art, last_leaf_ref, NType::LEAF);
			for (idx_t i = 0; i < leaf.count; i++) {
				result_ids.push_back(leaf.row_ids[i]);
			}
			last_leaf_ref = leaf.ptr;
		}
	}

	return true;
}

bool Leaf::ContainsRowId(ART &art, const Node &node, const row_t row_id) {

	D_ASSERT(node.HasMetadata());

	if (node.GetType() == NType::LEAF_INLINED) {
		return node.GetRowId() == row_id;
	}

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

string Leaf::VerifyAndToString(ART &art, const Node &node, const bool only_verify) {

	if (node.GetType() == NType::LEAF_INLINED) {
		return only_verify ? "" : "Leaf [count: 1, row ID: " + to_string(node.GetRowId()) + "]";
	}

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

void Leaf::Vacuum(ART &art, Node &node) {

	auto &allocator = Node::GetAllocator(art, NType::LEAF);

	// FIXME: Decrease fragmentation by combining non-full nodes.
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

void Leaf::MoveInlinedToLeaf(ART &art, Node &node) {

	D_ASSERT(node.GetType() == NType::LEAF_INLINED);
	auto row_id = node.GetRowId();
	auto &leaf = New(art, node);

	leaf.count = 1;
	leaf.row_ids[0] = row_id;
}

} // namespace duckdb
