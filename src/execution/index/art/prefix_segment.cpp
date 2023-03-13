#include "duckdb/execution/index/art/prefix_segment.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_node.hpp"

namespace duckdb {

PrefixSegment::PrefixSegment() : next(0) {
}

PrefixSegment *PrefixSegment::Initialize(ART &art, const idx_t &position) {
	auto segment = PrefixSegment::Get(art, position);
	art.IncreaseMemorySize(sizeof(PrefixSegment));

	segment->next = DConstants::INVALID_INDEX;
	return segment;
}

PrefixSegment *PrefixSegment::Append(ART &art, uint32_t &count, const uint8_t &byte) {

	auto *segment = this;
	auto position = count % ARTNode::PREFIX_SEGMENT_SIZE;

	// we need a new segment
	if (position == 0 && count != 0) {
		PrefixSegment::New(art, next);
		segment = PrefixSegment::Initialize(art, next);
	}

	segment->bytes[position] = byte;
	count++;
	return segment;
}

PrefixSegment *PrefixSegment::GetTail(ART &art) {

	auto segment = this;
	auto position = next;
	while (position != DConstants::INVALID_INDEX) {
		segment = PrefixSegment::Get(art, position);
		position = segment->next;
	}
	return segment;
}

} // namespace duckdb
