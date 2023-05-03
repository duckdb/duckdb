#include "duckdb/execution/index/art/prefix_segment.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

PrefixSegment &PrefixSegment::New(ART &art, Node &node) {

	node.SetPtr(Node::GetAllocator(art, NType::PREFIX_SEGMENT).New());
	node.type = (uint8_t)NType::PREFIX_SEGMENT;

	auto &segment = PrefixSegment::Get(art, node);
	segment.next.Reset();
	return segment;
}

PrefixSegment &PrefixSegment::Append(ART &art, uint32_t &count, const uint8_t byte) {

	reference<PrefixSegment> segment(*this);
	auto position = count % Node::PREFIX_SEGMENT_SIZE;

	// we need a new segment
	if (position == 0 && count != 0) {
		segment = PrefixSegment::New(art, next);
	}

	segment.get().bytes[position] = byte;
	count++;
	return segment.get();
}

PrefixSegment &PrefixSegment::GetTail(const ART &art) {

	reference<PrefixSegment> segment(*this);
	while (segment.get().next.IsSet()) {
		segment = PrefixSegment::Get(art, segment.get().next);
	}
	return segment.get();
}

} // namespace duckdb
