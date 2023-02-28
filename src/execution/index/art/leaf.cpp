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

	// possibly inline the row ID
	if (count == 2) {
		auto segment = art.leaf_segments.GetDataAtPosition<LeafSegment>(row_ids.position);
		if (segment->row_ids[0] != row_id && segment->row_ids[1] != row_id) {
			return;
		}

		auto temp_row_id = segment->row_ids[0] == row_id ? segment->row_ids[1] : segment->row_ids[0];

		art.DecreaseMemorySize(sizeof(LeafSegment));
		art.leaf_segments.FreePosition(row_ids.position);
		row_ids.inlined = temp_row_id;
		count--;
		return;
	}

	// find the row ID, and the segment containing that row ID
	auto position = row_ids.position;
	auto row_id_position = FindRowID(art, position, row_id);
	if (row_id_position == DConstants::INVALID_INDEX) {
		return;
	}

	// iterate all remaining segments and move the row IDs one field to the left
	LeafSegment *prev_segment = nullptr;
	while (position != DConstants::INVALID_INDEX) {

		auto segment = art.leaf_segments.GetDataAtPosition<LeafSegment>(position);
		if (prev_segment) {
			// this segment has at least one element, and we need to copy it into the previous segment
			prev_segment->row_ids[ARTNode::LEAF_SEGMENT_SIZE - 1] = segment->row_ids[0];
		}

		// move the data
		auto start = row_id_position % ARTNode::LEAF_SEGMENT_SIZE;
		auto len = std::min(count - row_id_position + 1, (idx_t)ARTNode::LEAF_SEGMENT_SIZE);
		memmove(segment->row_ids + start, segment->row_ids + start + 1, len);

		// adjust loop variables
		row_id_position += len;
		prev_segment = segment;
		position = segment->next;
	}

	// true, if we need to delete the last segment
	if (count % ARTNode::LEAF_SEGMENT_SIZE == 1) {
		position = row_ids.position;
		while (position != DConstants::INVALID_INDEX) {

			// get the segment succeeding the current segment
			auto segment = art.leaf_segments.GetDataAtPosition<LeafSegment>(position);
			D_ASSERT(segment->next != DConstants::INVALID_INDEX);
			auto next_segment = art.leaf_segments.GetDataAtPosition<LeafSegment>(segment->next);

			// the segment following next_segment is the tail of the segment list
			if (next_segment->next == DConstants::INVALID_INDEX) {
				art.DecreaseMemorySize(sizeof(LeafSegment));
				art.leaf_segments.FreePosition(segment->next);
				segment->next = DConstants::INVALID_INDEX;
			}

			// adjust loop variables
			position = segment->next;
		}
	}
	count--;
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

idx_t Leaf::FindRowID(ART &art, idx_t &position, const row_t &row_id) const {
	D_ASSERT(!IsInlined());

	auto next_position = position;
	auto remaining = count;
	while (next_position != DConstants::INVALID_INDEX) {

		position = next_position;
		auto segment = art.leaf_segments.GetDataAtPosition<LeafSegment>(next_position);
		auto search_count = std::min(remaining, ARTNode::LEAF_SEGMENT_SIZE);

		// search in this segment
		for (idx_t i = 0; i < search_count; i++) {
			if (segment->row_ids[i] == row_id) {
				return count - remaining + i;
			}
		}

		// adjust loop variables
		remaining -= search_count;
		next_position = segment->next;
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
