#include "duckdb/execution/index/art/leaf.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_node.hpp"
#include "duckdb/execution/index/art/leaf_segment.hpp"

namespace duckdb {

Leaf *Leaf::Initialize(ART &art, const ARTNode &node, const Key &key, const uint32_t &depth, const row_t &row_id) {
	auto leaf = art.leaf_nodes.GetDataAtPosition<Leaf>(node.GetPointer());
	art.IncreaseMemorySize(sizeof(Leaf));

	// set the fields of the leaf
	leaf->count = 1;
	leaf->row_ids.inlined = row_id;

	// initialize the prefix
	D_ASSERT(key.len >= depth);
	leaf->prefix.Initialize(art, key, depth, key.len - depth);

	return leaf;
}

Leaf *Leaf::Initialize(ART &art, const ARTNode &node, const Key &key, const uint32_t &depth, const row_t *row_ids,
                       const idx_t &count) {
	auto leaf = art.leaf_nodes.GetDataAtPosition<Leaf>(node.GetPointer());
	art.IncreaseMemorySize(sizeof(Leaf));

	// inlined leaf
	D_ASSERT(count >= 1);
	if (count == 1) {
		return Leaf::Initialize(art, node, key, depth, row_ids[0]);
	}

	// set the fields of the leaf
	leaf->count = 0;

	// copy the row IDs
	leaf->row_ids.position = art.leaf_segments.GetPosition();
	auto segment = LeafSegment::Initialize(art, leaf->row_ids.position);
	for (idx_t i = 0; i < count; i++) {
		segment = segment->Append(art, leaf->count, row_ids[i]);
	}

	// set the prefix
	D_ASSERT(key.len >= depth);
	leaf->prefix.Initialize(art, key, depth, key.len - depth);
}

void Leaf::Insert(ART &art, const row_t &row_id) {

	if (count == 0) {
		row_ids.inlined = row_id;
		count++;
		return;
	}

	if (count == 1) {
		MoveInlinedToSegment(art);
	}

	// get the segment to append to
	auto segment = art.leaf_segments.GetDataAtPosition<LeafSegment>(row_ids.position)->GetTail(art);
	segment->Append(art, count, row_id);
}

void Leaf::Remove(ART &art, const row_t &row_id) {

	if (count == 0) {
		return;
	}

	if (IsInlined()) {
		if (row_ids.inlined == row_id) {
			count--;
		}
		return;
	}

	// TODO: if true, then we have to later delete the tail
	auto delete_tail = count % ARTNode::LEAF_SEGMENT_SIZE == 1;

	// find the row ID, and the segment containing that row ID
	auto row_id_position = FindRowID(art, row_id);
	// TODO: wrong segment and position
	auto position = row_ids.position;
	auto segment = art.leaf_segments.GetDataAtPosition<LeafSegment>(row_ids.position);

	// iterate all segments and copy/shift their data
	auto copy_start = row_id_position + 1;
	auto remaining = count - copy_start;
	count = row_id_position;

	// TODO: super messy
	//	while (position) {
	//		auto other_segment = art.leaf_segments.GetDataAtPosition<LeafSegment>(position);
	//		auto copy_count = std::min(remaining, ARTNode::LEAF_SEGMENT_SIZE);
	//
	//		// now copy/shift the data
	//		for (idx_t i = 0; i < copy_count; i++) {
	//			segment = segment->Append(art, count, other_segment->row_ids[i]);
	//		}
	//
	//		// adjust the loop variables
	//		position = other_segment->next;
	//		remaining -= copy_count;
	//	}
	//	D_ASSERT(remaining == 0);

	if (delete_tail) {
		// TODO: get tail method, then use position to free
	}

	// possibly inline the row ID
	if (IsInlined()) {
	}
}

uint8_t Leaf::GetRowID(ART &art, const idx_t &position) const {

	D_ASSERT(position < count);
	if (IsInlined()) {
		return row_ids.inlined;
	}

	// get the correct segment
	auto segment = art.leaf_segments.GetDataAtPosition<LeafSegment>(row_ids.position);
	for (idx_t i = 0; i < position / ARTNode::LEAF_SEGMENT_SIZE; i++) {
		D_ASSERT(segment->next != DConstants::INVALID_INDEX);
		segment = art.leaf_segments.GetDataAtPosition<LeafSegment>(segment->next);
	}

	return segment->row_ids[position % ARTNode::PREFIX_SEGMENT_SIZE];
}

idx_t Leaf::FindRowID(ART &art, const row_t &row_id) const {
	D_ASSERT(!IsInlined());

	auto remaining = count;
	auto position = row_ids.position;

	while (position != DConstants::INVALID_INDEX) {
		auto segment = art.leaf_segments.GetDataAtPosition<LeafSegment>(position);
		auto find_count = std::min(remaining, ARTNode::LEAF_SEGMENT_SIZE);
		for (idx_t i = 0; i < find_count; i++) {
			if (segment->row_ids[i] == row_id) {
				return count - remaining + i;
			}
		}
		remaining -= find_count;
		position = segment->next;
	}
	return DConstants::INVALID_INDEX;
}

bool Leaf::IsInlined() const {
	return count <= 1;
}

void Leaf::MoveInlinedToSegment(ART &art) {
	D_ASSERT(IsInlined());
	auto position = art.leaf_segments.GetPosition();
	auto segment = LeafSegment::Initialize(art, position);

	// move row ID
	D_ASSERT(ARTNode::LEAF_SEGMENT_SIZE >= 1);
	segment->row_ids[0] = row_ids.inlined;
	row_ids.position = position;
}

} // namespace duckdb
