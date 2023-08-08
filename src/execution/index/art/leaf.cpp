#include "duckdb/execution/index/art/leaf.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/storage/meta_block_reader.hpp"
#include "duckdb/storage/meta_block_writer.hpp"

namespace duckdb {

void Leaf::New(Node &node, const row_t row_id) {

	// we directly inline this row ID into the node pointer
	D_ASSERT(row_id < MAX_ROW_ID_LOCAL);
	node.Reset();
	node.SetType((uint8_t)NType::LEAF_INLINED);
	node.SetRowId(row_id);
}

void Leaf::New(ART &art, reference<Node> &node, const row_t *row_ids, idx_t count) {

	D_ASSERT(count > 1);

	idx_t copy_count = 0;
	while (count) {
		node.get() = Node::GetAllocator(art, NType::LEAF).New();
		node.get().SetType((uint8_t)NType::LEAF);

		auto &leaf = Leaf::Get(art, node);

		leaf.count = MinValue((idx_t)Node::LEAF_SIZE, count);
		for (idx_t i = 0; i < leaf.count; i++) {
			leaf.row_ids[i] = row_ids[copy_count + i];
		}

		copy_count += leaf.count;
		count -= leaf.count;

		node = leaf.ptr;
		leaf.ptr.Reset();
	}
}

void Leaf::Free(ART &art, Node &node) {

	Node current_node = node;
	Node next_node;
	while (current_node.IsSet() && !current_node.IsSerialized()) {
		next_node = Leaf::Get(art, current_node).ptr;
		Node::GetAllocator(art, NType::LEAF).Free(current_node);
		current_node = next_node;
	}

	node.Reset();
}

void Leaf::InitializeMerge(ART &art, Node &node, const ARTFlags &flags) {

	auto merge_buffer_count = flags.merge_buffer_counts[(uint8_t)NType::LEAF - 1];

	Node next_node = node;
	node.AddToBufferID(merge_buffer_count);

	while (next_node.IsSet()) {
		auto &leaf = Leaf::Get(art, next_node);
		next_node = leaf.ptr;
		if (leaf.ptr.IsSet()) {
			leaf.ptr.AddToBufferID(merge_buffer_count);
		}
	}
}

void Leaf::Merge(ART &art, Node &l_node, Node &r_node) {

	D_ASSERT(l_node.IsSet() && !l_node.IsSerialized());
	D_ASSERT(r_node.IsSet() && !r_node.IsSerialized());

	// copy inlined row ID of r_node
	if (r_node.GetType() == NType::LEAF_INLINED) {
		Leaf::Insert(art, l_node, r_node.GetRowId());
		r_node.Reset();
		return;
	}

	// l_node has an inlined row ID, swap and insert
	if (l_node.GetType() == NType::LEAF_INLINED) {
		auto row_id = l_node.GetRowId();
		l_node = r_node;
		Leaf::Insert(art, l_node, row_id);
		r_node.Reset();
		return;
	}

	D_ASSERT(l_node.GetType() != NType::LEAF_INLINED);
	D_ASSERT(r_node.GetType() != NType::LEAF_INLINED);

	reference<Node> l_node_ref(l_node);
	reference<Leaf> l_leaf = Leaf::Get(art, l_node_ref);

	// find a non-full node
	while (l_leaf.get().count == Node::LEAF_SIZE) {
		l_node_ref = l_leaf.get().ptr;

		// the last leaf is full
		if (!l_leaf.get().ptr.IsSet()) {
			break;
		}
		l_leaf = Leaf::Get(art, l_node_ref);
	}

	// store the last leaf and then append r_node
	auto last_leaf_node = l_node_ref.get();
	l_node_ref.get() = r_node;
	r_node.Reset();

	// append the remaining row IDs of the last leaf node
	if (last_leaf_node.IsSet()) {
		// find the tail
		l_leaf = Leaf::Get(art, l_node_ref);
		while (l_leaf.get().ptr.IsSet()) {
			l_leaf = Leaf::Get(art, l_leaf.get().ptr);
		}
		// append the row IDs
		auto &last_leaf = Leaf::Get(art, last_leaf_node);
		for (idx_t i = 0; i < last_leaf.count; i++) {
			l_leaf = l_leaf.get().Append(art, last_leaf.row_ids[i]);
		}
		Node::GetAllocator(art, NType::LEAF).Free(last_leaf_node);
	}
}

void Leaf::Insert(ART &art, Node &node, const row_t row_id) {

	D_ASSERT(node.IsSet() && !node.IsSerialized());

	if (node.GetType() == NType::LEAF_INLINED) {
		Leaf::MoveInlinedToLeaf(art, node);
		Leaf::Insert(art, node, row_id);
		return;
	}

	// append to the tail
	reference<Leaf> leaf = Leaf::Get(art, node);
	while (leaf.get().ptr.IsSet()) {
		if (leaf.get().ptr.IsSerialized()) {
			leaf.get().ptr.Deserialize(art);
		}
		leaf = Leaf::Get(art, leaf.get().ptr);
	}
	leaf.get().Append(art, row_id);
}

bool Leaf::Remove(ART &art, reference<Node> &node, const row_t row_id) {

	D_ASSERT(node.get().IsSet() && !node.get().IsSerialized());

	if (node.get().GetType() == NType::LEAF_INLINED) {
		if (node.get().GetRowId() == row_id) {
			return true;
		}
		return false;
	}

	reference<Leaf> leaf = Leaf::Get(art, node);

	// inline the remaining row ID
	if (leaf.get().count == 2) {
		if (leaf.get().row_ids[0] == row_id || leaf.get().row_ids[1] == row_id) {
			auto remaining_row_id = leaf.get().row_ids[0] == row_id ? leaf.get().row_ids[1] : leaf.get().row_ids[0];
			Node::Free(art, node);
			Leaf::New(node, remaining_row_id);
		}
		return false;
	}

	// get the last row ID (the order within a leaf does not matter)
	// because we want to overwrite the row ID to remove with that one

	// go to the tail and keep track of the previous leaf node
	reference<Leaf> prev_leaf(leaf);
	while (leaf.get().ptr.IsSet()) {
		prev_leaf = leaf;
		if (leaf.get().ptr.IsSerialized()) {
			leaf.get().ptr.Deserialize(art);
		}
		leaf = Leaf::Get(art, leaf.get().ptr);
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
	while (node.get().IsSet()) {
		D_ASSERT(!node.get().IsSerialized());
		leaf = Leaf::Get(art, node);
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

idx_t Leaf::TotalCount(ART &art, Node &node) {

	// NOTE: first leaf in the leaf chain is already deserialized
	D_ASSERT(node.IsSet() && !node.IsSerialized());

	if (node.GetType() == NType::LEAF_INLINED) {
		return 1;
	}

	idx_t count = 0;
	reference<Node> node_ref(node);
	while (node_ref.get().IsSet()) {
		auto &leaf = Leaf::Get(art, node_ref);
		count += leaf.count;

		if (leaf.ptr.IsSerialized()) {
			leaf.ptr.Deserialize(art);
		}
		node_ref = leaf.ptr;
	}
	return count;
}

bool Leaf::GetRowIds(ART &art, Node &node, vector<row_t> &result_ids, idx_t max_count) {

	// adding more elements would exceed the maximum count
	D_ASSERT(node.IsSet());
	if (result_ids.size() + Leaf::TotalCount(art, node) > max_count) {
		return false;
	}

	// NOTE: Leaf::TotalCount fully deserializes the leaf
	D_ASSERT(!node.IsSerialized());

	if (node.GetType() == NType::LEAF_INLINED) {
		// push back the inlined row ID of this leaf
		result_ids.push_back(node.GetRowId());

	} else {
		// push back all the row IDs of this leaf
		reference<Node> last_leaf_ref(node);
		while (last_leaf_ref.get().IsSet()) {
			auto &leaf = Leaf::Get(art, last_leaf_ref);
			for (idx_t i = 0; i < leaf.count; i++) {
				result_ids.push_back(leaf.row_ids[i]);
			}

			D_ASSERT(!leaf.ptr.IsSerialized());
			last_leaf_ref = leaf.ptr;
		}
	}

	return true;
}

bool Leaf::ContainsRowId(ART &art, Node &node, const row_t row_id) {

	// NOTE: we either just removed a row ID from this leaf (by copying the
	// last row ID at a different position) or inserted a row ID into this leaf
	// (at the end), so the whole leaf is deserialized
	D_ASSERT(node.IsSet() && !node.IsSerialized());

	if (node.GetType() == NType::LEAF_INLINED) {
		return node.GetRowId() == row_id;
	}

	reference<Node> ref_node(node);
	while (ref_node.get().IsSet()) {
		auto &leaf = Leaf::Get(art, ref_node);
		for (idx_t i = 0; i < leaf.count; i++) {
			if (leaf.row_ids[i] == row_id) {
				return true;
			}
		}

		D_ASSERT(!leaf.ptr.IsSerialized());
		ref_node = leaf.ptr;
	}

	return false;
}

string Leaf::VerifyAndToString(ART &art, Node &node) {

	if (node.GetType() == NType::LEAF_INLINED) {
		return "Leaf [count: 1, row ID: " + to_string(node.GetRowId()) + "]";
	}

	string str = "";

	reference<Node> node_ref(node);
	while (node_ref.get().IsSet()) {

		auto &leaf = Leaf::Get(art, node_ref);
		D_ASSERT(leaf.count <= Node::LEAF_SIZE);

		str += "Leaf [count: " + to_string(leaf.count) + ", row IDs: ";
		for (idx_t i = 0; i < leaf.count; i++) {
			str += to_string(leaf.row_ids[i]) + "-";
		}
		str += "] ";

		// NOTE: we are currently only calling this function during CREATE INDEX
		// statements (and debugging), so the index is never serialized
		D_ASSERT(!leaf.ptr.IsSerialized());
		node_ref = leaf.ptr;
	}
	return str;
}

BlockPointer Leaf::Serialize(ART &art, Node &node, MetaBlockWriter &writer) {

	if (node.GetType() == NType::LEAF_INLINED) {
		auto block_pointer = writer.GetBlockPointer();
		writer.Write(NType::LEAF_INLINED);
		writer.Write(node.GetRowId());
		return block_pointer;
	}

	auto block_pointer = writer.GetBlockPointer();
	writer.Write(NType::LEAF);
	idx_t total_count = Leaf::TotalCount(art, node);
	writer.Write<idx_t>(total_count);

	// iterate all leaves and write their row IDs
	reference<Node> ref_node(node);
	while (ref_node.get().IsSet()) {
		D_ASSERT(!ref_node.get().IsSerialized());
		auto &leaf = Leaf::Get(art, ref_node);

		// write row IDs
		for (idx_t i = 0; i < leaf.count; i++) {
			writer.Write(leaf.row_ids[i]);
		}
		ref_node = leaf.ptr;
	}

	return block_pointer;
}

void Leaf::Deserialize(ART &art, Node &node, MetaBlockReader &reader) {

	auto total_count = reader.Read<idx_t>();
	reference<Node> ref_node(node);

	while (total_count) {
		ref_node.get() = Node::GetAllocator(art, NType::LEAF).New();
		ref_node.get().SetType((uint8_t)NType::LEAF);

		auto &leaf = Leaf::Get(art, ref_node);

		leaf.count = MinValue((idx_t)Node::LEAF_SIZE, total_count);
		for (idx_t i = 0; i < leaf.count; i++) {
			leaf.row_ids[i] = reader.Read<row_t>();
		}

		total_count -= leaf.count;
		ref_node = leaf.ptr;
		leaf.ptr.Reset();
	}
}

void Leaf::Vacuum(ART &art, Node &node) {

	auto &allocator = Node::GetAllocator(art, NType::LEAF);

	reference<Node> node_ref(node);
	while (node_ref.get().IsSet() && !node_ref.get().IsSerialized()) {
		if (allocator.NeedsVacuum(node_ref)) {
			node_ref.get() = allocator.VacuumPointer(node_ref);
			node_ref.get().SetType((uint8_t)NType::LEAF);
		}
		auto &leaf = Leaf::Get(art, node_ref);
		node_ref = leaf.ptr;
	}
}

void Leaf::MoveInlinedToLeaf(ART &art, Node &node) {

	D_ASSERT(node.GetType() == NType::LEAF_INLINED);
	auto row_id = node.GetRowId();
	node = Node::GetAllocator(art, NType::LEAF).New();
	node.SetType((uint8_t)NType::LEAF);

	auto &leaf = Leaf::Get(art, node);
	leaf.count = 1;
	leaf.row_ids[0] = row_id;
	leaf.ptr.Reset();
}

Leaf &Leaf::Append(ART &art, const row_t row_id) {

	reference<Leaf> leaf(*this);

	// we need a new leaf node
	if (leaf.get().count == Node::LEAF_SIZE) {
		leaf.get().ptr = Node::GetAllocator(art, NType::LEAF).New();
		leaf.get().ptr.SetType((uint8_t)NType::LEAF);

		leaf = Leaf::Get(art, leaf.get().ptr);
		leaf.get().count = 0;
		leaf.get().ptr.Reset();
	}

	leaf.get().row_ids[leaf.get().count] = row_id;
	leaf.get().count++;
	return leaf.get();
}

} // namespace duckdb
