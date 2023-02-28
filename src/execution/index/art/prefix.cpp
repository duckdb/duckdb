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

void Prefix::Initialize(ART &art, const Key &key, const uint32_t &depth, const uint32_t &count_p) {

	// prefix can be inlined
	if (count_p <= ARTNode::PREFIX_INLINE_BYTES) {
		memcpy(data.inlined, key.data + depth, count_p);
		count = count_p;
		return;
	}

	// prefix cannot be inlined, copy to segment(s)
	data.position = art.prefix_segments.GetPosition();
	auto segment = PrefixSegment::Initialize(art, data.position);
	for (idx_t i = 0; i < count_p; i++) {
		segment = segment->Append(art, count, key.data[depth + i]);
	}

	// NOTE: we do not increase the memory size for a prefix, because a prefix is assumed to be part
	// of another node, and already accounted for when tracking the memory size of that node
}

void Prefix::Move(Prefix &other) {
	count = other.count;
	data = other.data;
}

void Prefix::Append(ART &art, Prefix &other) {

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

	// get the tail of the segments of this prefix
	auto segment = art.prefix_segments.GetDataAtPosition<PrefixSegment>(data.position)->GetTail(art);

	// the other prefix is inlined
	if (other.IsInlined()) {
		for (idx_t i = 0; i < other.count; i++) {
			segment = segment->Append(art, count, other.data.inlined[i]);
		}
		return;
	}

	// iterate all segments of the other prefix and copy their data
	auto position = other.data.position;
	auto remaining = other.count;

	while (position) {
		auto other_segment = art.prefix_segments.GetDataAtPosition<PrefixSegment>(position);
		auto copy_count = std::min(remaining, ARTNode::PREFIX_SEGMENT_SIZE);

		// adjust the loop variables
		position = other_segment->next;
		remaining -= copy_count;

		// now copy the data
		for (idx_t i = 0; i < copy_count; i++) {
			segment = segment->Append(art, count, other_segment->bytes[i]);
		}
	}
	D_ASSERT(remaining == 0);
}

void Prefix::Concatenate(ART &art, const uint8_t &byte, Prefix &other) {

	auto new_size = count + 1 + other.count;

	// overwrite into this prefix
	if (new_size <= ARTNode::PREFIX_INLINE_BYTES) {
		// move this.prefix backwards
		for (idx_t i = 0; i < count; i++) {
			data.inlined[other.count + 1 + i] = GetByte(art, i);
		}
		// copy byte
		data.inlined[other.count] = byte;
		// copy other.prefix into this.prefix
		for (idx_t i = 0; i < other.count; i++) {
			data.inlined[i] = other.GetByte(art, i);
		}
		count = new_size;
		return;
	}

	// else, create a new prefix and then copy it into the existing prefix
	Prefix new_prefix;
	// append the other prefix
	new_prefix.Append(art, other);
	// copy the byte
	Prefix byte_prefix(byte);
	new_prefix.Append(art, byte_prefix);
	// copy the prefix
	new_prefix.Append(art, *this);
	// move the new_prefix into this prefix
	Move(new_prefix);
	return;
}

uint8_t Prefix::Reduce(ART &art, const idx_t &n) {

	auto new_count = count - n - 1;
	auto new_first_byte = GetByte(art, n);

	// prefix is now empty
	if (new_count == 0) {
		Delete(art);
		return new_first_byte;
	}

	// inlined, just move bytes
	if (IsInlined()) {
		for (idx_t i = 0; i < new_count; i++) {
			data.inlined[i] = data.inlined[i + n + 1];
		}
		count = new_count;
		return new_first_byte;
	}

	// iterate all segments and copy/shift their data
	auto segment = art.prefix_segments.GetDataAtPosition<PrefixSegment>(data.position);
	auto position = data.position;
	auto remaining = count;
	count = 0;

	while (position) {
		auto other_segment = art.prefix_segments.GetDataAtPosition<PrefixSegment>(position);
		auto copy_count = std::min(remaining, ARTNode::PREFIX_SEGMENT_SIZE);

		// now copy/shift the data
		for (idx_t i = 0; i < copy_count; i++) {
			segment = segment->Append(art, count, other_segment->bytes[i]);
		}

		// adjust the loop variables
		position = other_segment->next;
		remaining -= copy_count;
	}
	D_ASSERT(remaining == 0);
	return new_first_byte;
}

uint8_t Prefix::GetByte(ART &art, const idx_t &position) const {

	D_ASSERT(position < count);
	if (IsInlined()) {
		return data.inlined[position];
	}

	// get the correct segment
	auto segment = art.prefix_segments.GetDataAtPosition<PrefixSegment>(data.position);
	for (idx_t i = 0; i < position / ARTNode::PREFIX_SEGMENT_SIZE; i++) {
		D_ASSERT(segment->next != DConstants::INVALID_INDEX);
		segment = art.prefix_segments.GetDataAtPosition<PrefixSegment>(segment->next);
	}

	return segment->bytes[position % ARTNode::PREFIX_SEGMENT_SIZE];
}

uint32_t Prefix::KeyMismatchPosition(ART &art, const Key &key, const uint32_t &depth) const {

	D_ASSERT(depth + count < key.len);
	idx_t position = 0;

	for (; position < count; position++) {
		if (key[depth + position] != GetByte(art, position)) {
			return position;
		}
	}
	return position;
}

void Prefix::Serialize(ART &art, MetaBlockWriter &writer) {

	writer.Write(count);

	// write inlined data
	if (IsInlined()) {
		writer.WriteData(data.inlined, count);
		return;
	}

	D_ASSERT(data.position);
	auto position = data.position;
	auto remaining = count;

	// iterate all prefix segments and write their bytes
	while (position != DConstants::INVALID_INDEX) {
		auto segment = art.prefix_segments.GetDataAtPosition<PrefixSegment>(position);
		auto copy_count = std::min(remaining, ARTNode::PREFIX_SEGMENT_SIZE);

		// write the bytes
		writer.WriteData(segment->bytes, copy_count);

		// adjust loop variables
		remaining -= copy_count;
		position = segment->next;
	}
}

void Prefix::Deserialize(ART &art, MetaBlockReader &reader) {

	auto count_p = reader.Read<uint32_t>();

	// copy into inlined data
	if (count_p <= ARTNode::PREFIX_INLINE_BYTES) {
		reader.ReadData(data.inlined, count_p);
		count = count_p;
		return;
	}

	// copy into segments
	data.position = art.prefix_segments.GetPosition();
	auto segment = PrefixSegment::Initialize(art, data.position);
	for (idx_t i = 0; i < count_p; i++) {
		segment = segment->Append(art, count, reader.Read<uint8_t>());
	}
	D_ASSERT(count_p == count);
}

idx_t Prefix::MemorySize() {
#ifdef DEBUG
	if (IsInlined()) {
		return sizeof(*this);
	}
	return sizeof(*this) + ((count / ARTNode::PREFIX_SEGMENT_SIZE) + 1) * sizeof(PrefixSegment);
#endif
}

void Prefix::Delete(ART &art) {
	if (IsInlined()) {
		Initialize();
		return;
	}

	// delete all prefix segments
	auto position = data.position;
	while (position != DConstants::INVALID_INDEX) {
		auto next_position = art.prefix_segments.GetDataAtPosition<PrefixSegment>(position)->next;
		art.DecreaseMemorySize(sizeof(PrefixSegment));
		art.prefix_segments.FreePosition(position);
		position = next_position;
	}
	Initialize();
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
	memmove(segment->bytes, data.inlined, count);
	data.position = position;
}

} // namespace duckdb
