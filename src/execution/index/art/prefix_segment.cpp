#include "duckdb/execution/index/art/prefix_segment.hpp"

#include "duckdb/execution/index/art/art_node.hpp"

namespace duckdb {

PrefixSegment::PrefixSegment() : next(0) {
}

PrefixSegment *PrefixSegment::Initialize(ART &art, const idx_t &position) {
	auto segment = art.prefix_segments.Get<PrefixSegment>(position);
	art.IncreaseMemorySize(sizeof(PrefixSegment));

	segment->next = DConstants::INVALID_INDEX;
	return segment;
}

PrefixSegment *PrefixSegment::Append(ART &art, uint32_t &count, const uint8_t &byte) {

	auto *segment = this;
	auto pos = count % ARTNode::PREFIX_SEGMENT_SIZE;

	// we need a new segment
	if (pos == 0 && count != 0) {
		auto new_position = art.prefix_segments.New();
		next = new_position;
		segment = PrefixSegment::Initialize(art, new_position);
	}

	segment->bytes[pos] = byte;
	count++;
	return segment;
}

PrefixSegment *PrefixSegment::GetTail(ART &art) {
	auto segment = this;
	auto position = next;
	while (next != DConstants::INVALID_INDEX) {
		segment = art.prefix_segments.Get<PrefixSegment>(position);
		position = segment->next;
	}
	return segment;
}

} // namespace duckdb
