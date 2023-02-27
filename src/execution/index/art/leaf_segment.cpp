#include "duckdb/execution/index/art/leaf_segment.hpp"

namespace duckdb {

LeafSegment *LeafSegment::Initialize(ART &art, const idx_t &position) {
	auto segment = art.leaf_segments.GetDataAtPosition<LeafSegment>(position);
	art.IncreaseMemorySize(sizeof(LeafSegment));

	segment->next = DConstants::INVALID_INDEX;
	return segment;
}

LeafSegment *LeafSegment::Append(ART &art, uint32_t &count, const row_t &row_id) {

	auto *segment = this;
	auto pos = count % ARTNode::LEAF_SEGMENT_SIZE;

	// we need a new segment
	if (pos == 0 && count != 0) {
		auto new_position = art.leaf_segments.GetPosition();
		next = new_position;
		segment = LeafSegment::Initialize(art, new_position);
	}

	segment->row_ids[pos] = row_id;
	count++;
	return segment;
}

LeafSegment *LeafSegment::GetTail(ART &art) {
	auto segment = this;
	auto position = next;
	while (next != DConstants::INVALID_INDEX) {
		segment = art.leaf_segments.GetDataAtPosition<LeafSegment>(position);
		position = segment->next;
	}
	return segment;
}

} // namespace duckdb
