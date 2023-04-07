#include "duckdb/execution/index/art/leaf_segment.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_node.hpp"

namespace duckdb {

LeafSegment *LeafSegment::New(ART &art, ARTNode &node) {

	node.SetPtr(art.leaf_segments->New());
	node.type = (uint8_t)ARTNodeType::LEAF_SEGMENT;

	auto segment = LeafSegment::Get(art, node);
	segment->next.Reset();

	return segment;
}

LeafSegment *LeafSegment::Append(ART &art, uint32_t &count, const row_t row_id) {

	auto *segment = this;
	auto position = count % ARTNode::LEAF_SEGMENT_SIZE;

	// we need a new segment
	if (position == 0 && count != 0) {
		segment = LeafSegment::New(art, next);
	}

	segment->row_ids[position] = row_id;
	count++;
	return segment;
}

LeafSegment *LeafSegment::GetTail(const ART &art) {

	auto segment = this;
	while (segment->next.IsSet()) {
		segment = LeafSegment::Get(art, segment->next);
	}
	return segment;
}

} // namespace duckdb
