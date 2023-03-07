#include "duckdb/execution/index/art/prefix_segment.hpp"

#include "duckdb/execution/index/art/art_node.hpp"

namespace duckdb {

PrefixSegment::PrefixSegment() : next(0) {
}

void PrefixSegment::Free(ART &art, const idx_t &position) {
	D_ASSERT(art.nodes.find(ARTNodeType::PREFIX_SEGMENT) != art.nodes.end());
	art.nodes.at(ARTNodeType::PREFIX_SEGMENT).Free(position);
	art.DecreaseMemorySize(sizeof(PrefixSegment));
}

idx_t PrefixSegment::New(ART &art) {
	D_ASSERT(art.nodes.find(ARTNodeType::PREFIX_SEGMENT) != art.nodes.end());
	return art.nodes.at(ARTNodeType::PREFIX_SEGMENT).New();
}

PrefixSegment *PrefixSegment::Initialize(ART &art, const idx_t &position) {
	auto segment = PrefixSegment::Get(art, position);
	art.IncreaseMemorySize(sizeof(PrefixSegment));

	segment->next = DConstants::INVALID_INDEX;
	return segment;
}

PrefixSegment *PrefixSegment::Get(ART &art, const idx_t &position) {
	D_ASSERT(art.nodes.find(ARTNodeType::PREFIX_SEGMENT) != art.nodes.end());
	return art.nodes.at(ARTNodeType::PREFIX_SEGMENT).Get<PrefixSegment>(position);
}

PrefixSegment *PrefixSegment::Append(ART &art, uint32_t &count, const uint8_t &byte) {

	auto *segment = this;
	auto pos = count % ARTNode::PREFIX_SEGMENT_SIZE;

	// we need a new segment
	if (pos == 0 && count != 0) {
		auto new_position = PrefixSegment::New(art);
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
		segment = PrefixSegment::Get(art, position);
		position = segment->next;
	}
	return segment;
}

} // namespace duckdb
