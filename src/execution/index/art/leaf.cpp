#include "duckdb/execution/index/art/leaf.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/execution/index/art/leaf_segment.hpp"
#include "duckdb/storage/meta_block_reader.hpp"
#include "duckdb/storage/meta_block_writer.hpp"

namespace duckdb {

Leaf &Leaf::New(ART &art, Node &node, const row_t row_id) {

	node.SetPtr(Node::GetAllocator(art, NType::LEAF).New());
	node.type = (uint8_t)NType::LEAF;
	auto &leaf = Leaf::Get(art, node);

	// set the fields of the leaf
	leaf.count = 1;
	leaf.row_ids.inlined = row_id;
	return leaf;
}

Leaf &Leaf::New(ART &art, Node &node, const row_t *row_ids, const idx_t count) {

	D_ASSERT(count > 1);

	node.SetPtr(Node::GetAllocator(art, NType::LEAF).New());
	node.type = (uint8_t)NType::LEAF;
	auto &leaf = Leaf::Get(art, node);

	// reset the count to copy the row IDs
	leaf.count = 0;
	reference<LeafSegment> segment(LeafSegment::New(art, leaf.row_ids.ptr));
	for (idx_t i = 0; i < count; i++) {
		segment = segment.get().Append(art, leaf.count, row_ids[i]);
	}

	return leaf;
}

void Leaf::Free(ART &art, Node &node) {

	D_ASSERT(node.IsSet());
	D_ASSERT(!node.IsSwizzled());

	// free leaf segments
	auto &leaf = Leaf::Get(art, node);
	if (!leaf.IsInlined()) {
		Node::Free(art, leaf.row_ids.ptr);
	}
}

void Leaf::InitializeMerge(const ART &art, const idx_t buffer_count) {

	if (IsInlined()) {
		return;
	}

	reference<LeafSegment> segment(LeafSegment::Get(art, row_ids.ptr));
	row_ids.ptr.buffer_id += buffer_count;

	auto ptr = segment.get().next;
	while (ptr.IsSet()) {
		segment.get().next.buffer_id += buffer_count;
		segment = LeafSegment::Get(art, ptr);
		ptr = segment.get().next;
	}
}

void Leaf::Merge(ART &art, Node &other) {

	auto &other_leaf = Leaf::Get(art, other);

	// copy inlined row ID
	if (other_leaf.IsInlined()) {
		Insert(art, other_leaf.row_ids.inlined);
		Node::Free(art, other);
		return;
	}

	// row ID was inlined, move to a new segment
	if (IsInlined()) {
		auto row_id = row_ids.inlined;
		auto &segment = LeafSegment::New(art, row_ids.ptr);
		segment.row_ids[0] = row_id;
	}

	// get the first segment to copy to
	reference<LeafSegment> segment(LeafSegment::Get(art, row_ids.ptr).GetTail(art));

	// initialize loop variables
	auto other_ptr = other_leaf.row_ids.ptr;
	auto remaining = other_leaf.count;

	// copy row IDs
	while (other_ptr.IsSet()) {
		auto &other_segment = LeafSegment::Get(art, other_ptr);
		auto copy_count = MinValue(Node::LEAF_SEGMENT_SIZE, remaining);

		// copy the data
		for (idx_t i = 0; i < copy_count; i++) {
			segment = segment.get().Append(art, count, other_segment.row_ids[i]);
		}

		// adjust the loop variables
		other_ptr = other_segment.next;
		remaining -= copy_count;
	}
	D_ASSERT(remaining == 0);

	Node::Free(art, other);
}

void Leaf::Insert(ART &art, const row_t row_id) {

	D_ASSERT(count != 0);
	if (count == 1) {
		MoveInlinedToSegment(art);
	}

	// append to the tail
	auto &first_segment = LeafSegment::Get(art, row_ids.ptr);
	auto &tail = first_segment.GetTail(art);
	tail.Append(art, count, row_id);
}

void Leaf::Remove(ART &art, const row_t row_id) {

	if (count == 0) {
		return;
	}

	if (IsInlined()) {
		if (row_ids.inlined == row_id) {
			count--;
		}
		return;
	}

	// possibly inline the row ID
	if (count == 2) {
		auto &segment = LeafSegment::Get(art, row_ids.ptr);
		D_ASSERT(segment.row_ids[0] == row_id || segment.row_ids[1] == row_id);
		auto remaining_row_id = segment.row_ids[0] == row_id ? segment.row_ids[1] : segment.row_ids[0];
		Node::Free(art, row_ids.ptr);
		row_ids.inlined = remaining_row_id;
		count--;
		return;
	}

	// find the row ID, and the segment containing that row ID (stored in ptr)
	auto ptr = row_ids.ptr;
	auto copy_idx = FindRowId(art, ptr, row_id);
	D_ASSERT(copy_idx != (uint32_t)DConstants::INVALID_INDEX);
	copy_idx++;

	// iterate all remaining segments and move the row IDs one field to the left
	reference<LeafSegment> segment(LeafSegment::Get(art, ptr));
	reference<LeafSegment> prev_segment(LeafSegment::Get(art, ptr));
	while (copy_idx < count) {

		auto copy_start = copy_idx % Node::LEAF_SEGMENT_SIZE;
		D_ASSERT(copy_start != 0);
		auto copy_end = MinValue(copy_start + count - copy_idx, Node::LEAF_SEGMENT_SIZE);

		// copy row IDs
		for (idx_t i = copy_start; i < copy_end; i++) {
			segment.get().row_ids[i - 1] = segment.get().row_ids[i];
			copy_idx++;
		}

		// adjust loop variables
		if (segment.get().next.IsSet()) {
			prev_segment = segment;
			segment = LeafSegment::Get(art, segment.get().next);
			// this segment has at least one element, and we need to copy it into the previous segment
			prev_segment.get().row_ids[Node::LEAF_SEGMENT_SIZE - 1] = segment.get().row_ids[0];
			copy_idx++;
		}
	}

	// this evaluates to true, if we need to delete the last segment
	if (count % Node::LEAF_SEGMENT_SIZE == 1) {
		ptr = row_ids.ptr;
		while (ptr.IsSet()) {

			// get the segment succeeding the current segment
			auto &current_segment = LeafSegment::Get(art, ptr);
			D_ASSERT(current_segment.next.IsSet());
			auto &next_segment = LeafSegment::Get(art, current_segment.next);

			// next_segment is the tail of the segment list
			if (!next_segment.next.IsSet()) {
				Node::Free(art, current_segment.next);
			}

			// adjust loop variables
			ptr = current_segment.next;
		}
	}
	count--;
}

row_t Leaf::GetRowId(const ART &art, const idx_t position) const {

	D_ASSERT(position < count);
	if (IsInlined()) {
		return row_ids.inlined;
	}

	// get the correct segment
	reference<LeafSegment> segment(LeafSegment::Get(art, row_ids.ptr));
	for (idx_t i = 0; i < position / Node::LEAF_SEGMENT_SIZE; i++) {
		D_ASSERT(segment.get().next.IsSet());
		segment = LeafSegment::Get(art, segment.get().next);
	}

	return segment.get().row_ids[position % Node::LEAF_SEGMENT_SIZE];
}

uint32_t Leaf::FindRowId(const ART &art, Node &ptr, const row_t row_id) const {

	D_ASSERT(!IsInlined());

	auto remaining = count;
	while (ptr.IsSet()) {

		auto &segment = LeafSegment::Get(art, ptr);
		auto search_count = MinValue(Node::LEAF_SEGMENT_SIZE, remaining);

		// search in this segment
		for (idx_t i = 0; i < search_count; i++) {
			if (segment.row_ids[i] == row_id) {
				return count - remaining + i;
			}
		}

		// adjust loop variables
		remaining -= search_count;
		ptr = segment.next;
	}
	return (uint32_t)DConstants::INVALID_INDEX;
}

string Leaf::VerifyAndToString(const ART &art, const bool only_verify) const {

	if (IsInlined()) {
		return only_verify ? "" : "Leaf [count: 1, row ID: " + to_string(row_ids.inlined) + "]";
	}

	auto ptr = row_ids.ptr;
	auto remaining = count;
	string str = "";
	uint32_t this_count = 0;
	while (ptr.IsSet()) {
		auto &segment = LeafSegment::Get(art, ptr);
		auto to_string_count = Node::LEAF_SEGMENT_SIZE < remaining ? Node::LEAF_SEGMENT_SIZE : remaining;

		for (idx_t i = 0; i < to_string_count; i++) {
			str += ", " + to_string(segment.row_ids[i]);
			this_count++;
		}
		remaining -= to_string_count;
		ptr = segment.next;
	}

	D_ASSERT(remaining == 0);
	(void)this_count;
	D_ASSERT(this_count == count);
	return only_verify ? "" : "Leaf [count: " + to_string(count) + ", row IDs: " + str + "] \n";
}

BlockPointer Leaf::Serialize(const ART &art, MetaBlockWriter &writer) const {

	// get pointer and write fields
	auto block_pointer = writer.GetBlockPointer();
	writer.Write(NType::LEAF);
	writer.Write<uint32_t>(count);

	if (IsInlined()) {
		writer.Write(row_ids.inlined);
		return block_pointer;
	}

	D_ASSERT(row_ids.ptr.IsSet());
	auto ptr = row_ids.ptr;
	auto remaining = count;

	// iterate all leaf segments and write their row IDs
	while (ptr.IsSet()) {
		auto &segment = LeafSegment::Get(art, ptr);
		auto write_count = MinValue(Node::LEAF_SEGMENT_SIZE, remaining);

		// write the row IDs
		for (idx_t i = 0; i < write_count; i++) {
			writer.Write(segment.row_ids[i]);
		}

		// adjust loop variables
		remaining -= write_count;
		ptr = segment.next;
	}
	D_ASSERT(remaining == 0);

	return block_pointer;
}

void Leaf::Deserialize(ART &art, MetaBlockReader &reader) {

	auto count_p = reader.Read<uint32_t>();

	// inlined
	if (count_p == 1) {
		row_ids.inlined = reader.Read<row_t>();
		count = count_p;
		return;
	}

	// copy into segments
	count = 0;
	reference<LeafSegment> segment(LeafSegment::New(art, row_ids.ptr));
	for (idx_t i = 0; i < count_p; i++) {
		segment = segment.get().Append(art, count, reader.Read<row_t>());
	}
	D_ASSERT(count_p == count);
}

void Leaf::Vacuum(ART &art) {

	if (IsInlined()) {
		return;
	}

	// first pointer has special treatment because we don't obtain it from a leaf segment
	auto &allocator = Node::GetAllocator(art, NType::LEAF_SEGMENT);
	if (allocator.NeedsVacuum(row_ids.ptr)) {
		row_ids.ptr.SetPtr(allocator.VacuumPointer(row_ids.ptr));
		row_ids.ptr.type = (uint8_t)NType::LEAF_SEGMENT;
	}

	auto ptr = row_ids.ptr;
	while (ptr.IsSet()) {
		auto &segment = LeafSegment::Get(art, ptr);
		ptr = segment.next;
		if (ptr.IsSet() && allocator.NeedsVacuum(ptr)) {
			segment.next.SetPtr(allocator.VacuumPointer(ptr));
			segment.next.type = (uint8_t)NType::LEAF_SEGMENT;
			ptr = segment.next;
		}
	}
}

void Leaf::MoveInlinedToSegment(ART &art) {

	D_ASSERT(IsInlined());

	auto row_id = row_ids.inlined;
	auto &segment = LeafSegment::New(art, row_ids.ptr);
	segment.row_ids[0] = row_id;
}

} // namespace duckdb
