#include "duckdb/execution/index/art/art_builder.hpp"

#include "duckdb/common/optional.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/prefix_handle.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/art_operator.hpp"

namespace duckdb {

ARTConflictType ARTBuilder::Build() {
	while (!s.empty()) {
		// Copy the entry so we can pop it.
		auto entry = s.top();
		s.pop();

		D_ASSERT(entry.start < keys.size());
		D_ASSERT(entry.end < keys.size());
		D_ASSERT(entry.start <= entry.end);

		auto &start = keys[entry.start];
		auto &end = keys[entry.end];
		D_ASSERT(start.len != 0);

		// Increment the depth until we reach a leaf or find a mismatching byte.
		auto prefix_depth = entry.depth;
		while (start.len != entry.depth && start.ByteMatches(end, entry.depth)) {
			entry.depth++;
		}

		// True, if we reached a leaf: all bytes of start_key and end_key match.
		auto is_leaf = start.len == entry.depth;
		auto row_id_count = entry.end - entry.start + 1;
		if (is_leaf && art.IsUnique() && row_id_count != 1) {
			return ARTConflictType::CONSTRAINT;
		}

		// Create the prefix, if any. The tail stays pinned while we fill its child.
		reference<NodePtr> node_ptr_ref(entry.node_ptr);
		optional<PrefixHandle> tail;
		auto prefix_length = entry.depth - prefix_depth;
		if (prefix_length != 0) {
			auto chain = PrefixHandle::New(art, start, prefix_depth, prefix_length);
			entry.node_ptr = chain.root_ptr;
			tail = std::move(chain.tail);
			node_ptr_ref = tail->Child(art);
		}

		if (is_leaf) {
			// Inline the row ID.
			if (row_id_count == 1) {
				Leaf::New(node_ptr_ref, row_ids[entry.start].GetRowId());
				continue;
			}

			// Loop and insert the row IDs.
			// We cannot iterate into the nested leaf with the builder
			// because row IDs are not sorted.
			for (idx_t i = entry.start; i < entry.start + row_id_count; i++) {
				ARTOperator::Insert(arena, art, node_ptr_ref, row_ids[i], 0, row_ids[i], GateStatus::GATE_SET,
				                    DeleteIndexInfo(), IndexAppendMode::DEFAULT);
			}
			node_ptr_ref.get().SetGateStatus(GateStatus::GATE_SET);
			continue;
		}

		vector<idx_t> child_offsets;
		child_offsets.emplace_back(entry.start);
		for (idx_t i = entry.start + 1; i <= entry.end; i++) {
			if (keys[i - 1].data[entry.depth] != keys[i].data[entry.depth]) {
				child_offsets.emplace_back(i);
			}
		}

		// Create a new node containing the children.
		NodePtr::New(art, node_ptr_ref, NodePtr::GetInternalNodeType(child_offsets.size()));
		auto start_offset = child_offsets[0];
		for (idx_t i = 1; i <= child_offsets.size(); i++) {
			auto child_byte = keys[start_offset].data[entry.depth];
			// FIXME: Improve performance by either returning a reference to the child directly,
			// FIXME: or by calling InsertChild after processing the child (at the end of the stack loop).
			NodePtr::InsertChild(art, node_ptr_ref, child_byte);
			auto child_ptr_ref = node_ptr_ref.get().NodePtr::GetChildMutableUnsafe(art, child_byte, true);
			auto end_offset = i != child_offsets.size() ? child_offsets[i] - 1 : entry.end;
			s.emplace(*child_ptr_ref, start_offset, end_offset, entry.depth + 1);
			start_offset = end_offset + 1;
		}
	}

	// We exhausted the stack.
	return ARTConflictType::NO_CONFLICT;
}

} // namespace duckdb
