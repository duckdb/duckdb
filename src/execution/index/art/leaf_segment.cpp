#include "duckdb/execution/index/art/leaf_segment.hpp"

#include "duckdb/execution/index/art/art_node.hpp"

namespace duckdb {

void LeafSegment::Free(ART &art, const idx_t &position) {
	D_ASSERT(art.nodes.find(ARTNodeType::LEAF_SEGMENT) != art.nodes.end());
	art.nodes.at(ARTNodeType::LEAF_SEGMENT).Free(position);
}

idx_t LeafSegment::New(ART &art) {
	D_ASSERT(art.nodes.find(ARTNodeType::LEAF_SEGMENT) != art.nodes.end());
	return art.nodes.at(ARTNodeType::LEAF_SEGMENT).New();
}

LeafSegment *LeafSegment::Initialize(ART &art, const idx_t &position) {
	auto segment = LeafSegment::Get(art, position);
	art.IncreaseMemorySize(sizeof(LeafSegment));

	segment->next = DConstants::INVALID_INDEX;
	return segment;
}

LeafSegment *LeafSegment::Get(ART &art, const idx_t &position) {
	D_ASSERT(art.nodes.find(ARTNodeType::LEAF_SEGMENT) != art.nodes.end());
	return art.nodes.at(ARTNodeType::LEAF_SEGMENT).Get<LeafSegment>(position);
}

LeafSegment *LeafSegment::Append(ART &art, uint32_t &count, const row_t &row_id) {

	auto *segment = this;
	auto pos = count % ARTNode::LEAF_SEGMENT_SIZE;

	// we need a new segment
	if (pos == 0 && count != 0) {
		auto new_position = LeafSegment::New(art);
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
		segment = LeafSegment::Get(art, position);
		position = segment->next;
	}
	return segment;
}

} // namespace duckdb
