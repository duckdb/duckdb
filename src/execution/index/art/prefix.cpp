#include "duckdb/execution/index/art/prefix.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_node.hpp"
#include "duckdb/execution/index/art/prefix_segment.hpp"

namespace duckdb {

Prefix::Prefix() : count(0) {
}

Prefix::Prefix(const uint8_t &byte) : count(1) {
	data.inlined[0] = byte;
}

void Prefix::Initialize() {
	count = 0;
}

uint8_t Prefix::GetData(ART &art, const idx_t &pos) {

	D_ASSERT(pos < count);
	if (IsInlined()) {
		return data.inlined[pos];
	}

	// get the prefix segment containing pos
	idx_t segment_count = 0;
	auto segment = art.prefix_segments.GetDataAtPosition<PrefixSegment>(data.position);
	while (pos >= segment_count * ARTNode::PREFIX_SEGMENT_SIZE) {
		D_ASSERT(segment->next != DConstants::INVALID_INDEX);
		segment = art.prefix_segments.GetDataAtPosition<PrefixSegment>(segment->next);
		segment_count++;
	}

	return segment->bytes[pos - segment_count * ARTNode::PREFIX_SEGMENT_SIZE];
}

void Prefix::Move(Prefix &other) {
	count = other.count;
	data = other.data;
}

void Prefix::Append(ART &art, ART &other_art, Prefix &other) {

	// result fits into inlined data, i.e., both prefixes are also inlined
	if (count + other.count <= ARTNode::PREFIX_INLINE_BYTES) {
		memcpy(data.inlined + count, other.data.inlined, other.count);
		count += other.count;
		return;
	}

	// this prefix is inlined, but will no longer be after appending the other prefix,
	// move the inlined bytes to the first prefix segment
	if (IsInlined()) {
		MoveInlinedToSegment(art);
	}

	// TODO: maybe this can be optimized a bit, because in theory only the first segment has to be filled up,
	// TODO: after that, we always copy into an empty segment

	PrefixSegment *other_segment;
	PrefixSegment temp_segment;
	if (other.IsInlined()) {
		// if the other prefix is inlined, then we copy its data into a temporary prefix segment
		memcpy(temp_segment.bytes, other.data.inlined, other.count);
		other_segment = &temp_segment;
	} else {
		// get the first prefix segment of the other prefix
		other_segment = other_art.prefix_segments.GetDataAtPosition<PrefixSegment>(other.data.position);
	}

	idx_t copy_count = 0;
	auto segment = PrefixSegment::GetTail(art, data.position);

	// copy all the data of the other prefix into this prefix
	while (copy_count < other.count) {
		// get copy starting positions and copy length
		idx_t free_in_this = count % ARTNode::PREFIX_SEGMENT_SIZE;
		auto start_in_this = ARTNode::PREFIX_SEGMENT_SIZE - free_in_this;
		auto start_in_other = copy_count % ARTNode::PREFIX_SEGMENT_SIZE;
		auto copy_from_other = ARTNode::PREFIX_SEGMENT_SIZE - start_in_other;

		// copy data
		auto this_copy_count = std::min(free_in_this, copy_from_other);
		for (idx_t i = start_in_other; i < this_copy_count; i++) {
			segment->bytes[start_in_this + i] = other_segment->bytes[start_in_other + i];
		}

		// adjust counters
		copy_count += this_copy_count;
		count += this_copy_count;

		// get next prefix segments, if necessary
		if (copy_count < other.count) {
			// we need to append a prefix segment to this prefix
			if (start_in_this + this_copy_count == ARTNode::PREFIX_SEGMENT_SIZE) {
				auto new_position = art.prefix_segments.GetPosition();
				segment->next = new_position;
				segment = PrefixSegment::Initialize(art, new_position);
			}
			// we need to copy the next prefix segment in the other prefix
			if (start_in_other + this_copy_count == ARTNode::PREFIX_SEGMENT_SIZE) {
				D_ASSERT(other_segment->next != DConstants::INVALID_INDEX);
				other_segment = other_art.prefix_segments.GetDataAtPosition<PrefixSegment>(other_segment->next);
			}
		}
	}
}

void Prefix::Concatenate(ART &art, const uint8_t &byte, ART &other_art, Prefix &other) {

	auto new_size = count + 1 + other.count;

	// overwrite into this prefix
	if (new_size <= ARTNode::PREFIX_INLINE_BYTES) {
		// move this.prefix backwards
		for (idx_t i = 0; i < count; i++) {
			data.inlined[other.count + 1 + i] = GetData(art, i);
		}
		// copy byte
		data.inlined[other.count] = byte;
		// copy other.prefix into this.prefix
		for (idx_t i = 0; i < other.count; i++) {
			data.inlined[i] = other.GetData(other_art, i);
		}
		count = new_size;
		return;
	}

	// else, create a new prefix and then copy it into the existing prefix
	Prefix new_prefix;
	// append the other prefix
	new_prefix.Append(art, other_art, other);
	// copy the byte
	Prefix byte_prefix(byte);
	new_prefix.Append(art, art, byte_prefix);
	// copy the prefix
	new_prefix.Append(art, art, *this);
	// move the new_prefix into this prefix
	Move(new_prefix);
	return;
}

void Prefix::Deserialize(ART &art, MetaBlockReader &reader) {

	count = reader.Read<uint32_t>();

	if (count <= ARTNode::PREFIX_INLINE_BYTES) {
		for (idx_t i = 0; i < count; i++) {
			data.inlined[i] = reader.Read<uint8_t>();
		}
		return;
	}

	// copy into prefix segment(s)
	idx_t copy_count = 0;
	data.position = DConstants::INVALID_INDEX;
	PrefixSegment *segment;

	while (copy_count != count) {

		// we need a new segment
		if (data.position == DConstants::INVALID_INDEX) {
			auto position = art.prefix_segments.GetPosition();
			data.position = position;
			segment = PrefixSegment::Initialize(art, position);
		}

		D_ASSERT(count >= copy_count);
		idx_t remaining = count - copy_count;
		auto this_copy_count = std::min((idx_t)ARTNode::PREFIX_SEGMENT_SIZE, remaining);
		for (idx_t i = 0; i < this_copy_count; i++) {
			segment->bytes[i] = reader.Read<uint8_t>();
		}

		copy_count += this_copy_count;
	}
}

idx_t Prefix::MemorySize() {
#ifdef DEBUG
	if (IsInlined()) {
		return sizeof(*this);
	}
	return sizeof(*this) + ((count / ARTNode::PREFIX_SEGMENT_SIZE) + 1) * sizeof(PrefixSegment);
#endif
}

bool Prefix::IsInlined() const {
	return count <= ARTNode::PREFIX_INLINE_BYTES;
}

void Prefix::MoveInlinedToSegment(ART &art) {
	D_ASSERT(IsInlined());
	auto position = art.prefix_segments.GetPosition();
	auto segment = PrefixSegment::Initialize(art, position);

	// move data
	D_ASSERT(ARTNode::PREFIX_SEGMENT_SIZE >= ARTNode::PREFIX_INLINE_BYTES);
	for (idx_t i = 0; i < count; i++) {
		segment->bytes[i] = data.inlined[i];
	}
	data.position = position;
}

} // namespace duckdb
