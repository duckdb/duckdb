#include "duckdb/execution/index/art/leaf_segment.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_node.hpp"

namespace duckdb {

LeafSegment *LeafSegment::Initialize(ART &art, const idx_t &position) {
	auto segment = LeafSegment::Get(art, position);
	art.IncreaseMemorySize(sizeof(LeafSegment));

	segment->next = DConstants::INVALID_INDEX;
	return segment;
}

LeafSegment *LeafSegment::Append(ART &art, uint32_t &count, const row_t &row_id) {

	auto *segment = this;
	auto position = count % ARTNode::LEAF_SEGMENT_SIZE;

	// we need a new segment
	if (position == 0 && count != 0) {
		LeafSegment::New(art, next);
		segment = LeafSegment::Initialize(art, next);
	}

	segment->row_ids[position] = row_id;
	count++;
	return segment;
}

LeafSegment *LeafSegment::GetTail(ART &art) {

	auto segment = this;
	auto position = next;
	while (position != DConstants::INVALID_INDEX) {
		segment = LeafSegment::Get(art, position);
		position = segment->next;
	}
	return segment;
}

} // namespace duckdb
