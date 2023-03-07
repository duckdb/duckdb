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

void Prefix::Free(ART &art) {
	if (IsInlined()) {
		Initialize();
		return;
	}

	// delete all prefix segments
	auto position = data.position;
	while (position != DConstants::INVALID_INDEX) {
		auto next_position = PrefixSegment::Get(art, position)->next;
		art.DecreaseMemorySize(sizeof(PrefixSegment));
		PrefixSegment::Free(art, position);
		position = next_position;
	}
	Initialize();
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
	count = 0;
	data.position = PrefixSegment::New(art);
	auto segment = PrefixSegment::Initialize(art, data.position);
	for (idx_t i = 0; i < count_p; i++) {
		segment = segment->Append(art, count, key.data[depth + i]);
	}

	// NOTE: we do not increase the memory size for a prefix, because a prefix is assumed to be part
	// of another node, and already accounted for when tracking the memory size of that node
}

void Prefix::Initialize(ART &art, const Prefix &other, const uint32_t &count_p) {

	D_ASSERT(count_p <= other.count);

	// copy inlined data
	if (other.IsInlined()) {
		memcpy(data.inlined, other.data.inlined, count_p);
		count = count_p;
		return;
	}

	// initialize the count and get the first segment
	count = 0;
	data.position = PrefixSegment::New(art);
	auto segment = PrefixSegment::Initialize(art, data.position);

	// iterate the segments of the other prefix and copy their data
	auto position = other.data.position;
	auto remaining = count_p;

	while (remaining != 0) {
		D_ASSERT(position != DConstants::INVALID_INDEX);
		auto other_segment = PrefixSegment::Get(art, position);
		auto copy_count = ARTNode::PREFIX_SEGMENT_SIZE < remaining ? ARTNode::PREFIX_SEGMENT_SIZE : remaining;

		// copy the data
		for (idx_t i = 0; i < copy_count; i++) {
			segment = segment->Append(art, count, other_segment->bytes[i]);
		}

		// adjust the loop variables
		position = other_segment->next;
		remaining -= copy_count;
	}
	D_ASSERT(count == count_p);
}

void Prefix::Vacuum(ART &art) {

	if (IsInlined()) {
		return;
	}

	// first position has special treatment because we don't obtain it from a leaf segment
	D_ASSERT(art.nodes.find(ARTNodeType::PREFIX_SEGMENT) != art.nodes.end());
	auto &allocator = art.nodes.at(ARTNodeType::PREFIX_SEGMENT);
	if (allocator.NeedsVacuum(data.position)) {
		data.position = allocator.Vacuum(data.position);
	}

	auto position = data.position;
	while (position != DConstants::INVALID_INDEX) {
		auto segment = PrefixSegment::Get(art, position);
		if (segment->next != DConstants::INVALID_INDEX && allocator.NeedsVacuum(segment->next)) {
			segment->next = allocator.Vacuum(segment->next);
		}
		position = segment->next;
	}
}

void Prefix::InitializeMerge(ART &art, const idx_t &buffer_count) {

	if (IsInlined()) {
		return;
	}

	auto position = data.position;
	D_ASSERT((data.position & FixedSizeAllocator::BUFFER_ID_TO_ZERO) ==
	         ((data.position + buffer_count) & FixedSizeAllocator::BUFFER_ID_TO_ZERO));
	data.position += buffer_count;

	while (position != DConstants::INVALID_INDEX) {
		auto segment = PrefixSegment::Get(art, position);
		position = segment->next;
		D_ASSERT((segment->next & FixedSizeAllocator::BUFFER_ID_TO_ZERO) ==
		         ((segment->next + buffer_count) & FixedSizeAllocator::BUFFER_ID_TO_ZERO));
		segment->next += buffer_count;
	}
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
	auto segment = PrefixSegment::Get(art, data.position)->GetTail(art);

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

	while (position != DConstants::INVALID_INDEX) {
		auto other_segment = PrefixSegment::Get(art, position);
		auto copy_count = ARTNode::PREFIX_SEGMENT_SIZE < remaining ? ARTNode::PREFIX_SEGMENT_SIZE : remaining;

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
		Free(art);
		return new_first_byte;
	}

	// was inlined, just move bytes
	if (IsInlined()) {
		for (idx_t i = 0; i < new_count; i++) {
			data.inlined[i] = data.inlined[i + n + 1];
		}
		count = new_count;
		return new_first_byte;
	}

	count = 0;
	auto start = n + 1;
	auto offset = start % ARTNode::PREFIX_SEGMENT_SIZE;
	auto remaining = new_count;

	// get the source segment, i.e., the segment that contains the byte at start
	auto source_segment = PrefixSegment::Get(art, data.position);
	for (idx_t i = 0; i < start / ARTNode::PREFIX_SEGMENT_SIZE; i++) {
		D_ASSERT(source_segment->next != DConstants::INVALID_INDEX);
		source_segment = PrefixSegment::Get(art, source_segment->next);
	}

	// iterate all segments and copy/shift their data
	auto destination_segment = PrefixSegment::Get(art, data.position);
	while (true) {
		auto copy_count = ARTNode::PREFIX_SEGMENT_SIZE - offset;
		copy_count = copy_count < remaining ? copy_count : remaining;

		// now copy/shift the data
		for (idx_t i = offset; i < offset + copy_count; i++) {
			destination_segment = destination_segment->Append(art, count, source_segment->bytes[i]);
		}

		// adjust the loop variables
		offset = 0;
		remaining -= copy_count;
		if (remaining == 0) {
			break;
		}
		D_ASSERT(source_segment->next != DConstants::INVALID_INDEX);
		source_segment = PrefixSegment::Get(art, source_segment->next);
	}
	return new_first_byte;
}

uint8_t Prefix::GetByte(ART &art, const idx_t &position) const {

	D_ASSERT(position < count);
	if (IsInlined()) {
		return data.inlined[position];
	}

	// get the correct segment
	auto segment = PrefixSegment::Get(art, data.position);
	for (idx_t i = 0; i < position / ARTNode::PREFIX_SEGMENT_SIZE; i++) {
		D_ASSERT(segment->next != DConstants::INVALID_INDEX);
		segment = PrefixSegment::Get(art, segment->next);
	}

	return segment->bytes[position % ARTNode::PREFIX_SEGMENT_SIZE];
}

uint32_t Prefix::KeyMismatchPosition(ART &art, const Key &key, const uint32_t &depth) const {

	D_ASSERT(depth + count < key.len);

	// FIXME: inefficient, because GetByte always traverses all segments
	for (idx_t position = 0; position < count; position++) {
		if (key[depth + position] != GetByte(art, position)) {
			return position;
		}
	}
	return count;
}

uint32_t Prefix::MismatchPosition(ART &art, const Prefix &other) const {

	D_ASSERT(count <= other.count);

	// FIXME: inefficient, because GetByte always traverses all segments
	for (uint32_t i = 0; i < count; i++) {
		if (GetByte(art, i) != other.GetByte(art, i)) {
			return i;
		}
	}
	return count;
}

void Prefix::Serialize(ART &art, MetaBlockWriter &writer) {

	writer.Write(count);

	// write inlined data
	if (IsInlined()) {
		writer.WriteData(data.inlined, count);
		return;
	}

	D_ASSERT(data.position != DConstants::INVALID_INDEX);
	auto position = data.position;
	auto remaining = count;

	// iterate all prefix segments and write their bytes
	while (position != DConstants::INVALID_INDEX) {
		auto segment = PrefixSegment::Get(art, position);
		auto copy_count = ARTNode::PREFIX_SEGMENT_SIZE < remaining ? ARTNode::PREFIX_SEGMENT_SIZE : remaining;

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
	count = 0;
	data.position = PrefixSegment::New(art);
	auto segment = PrefixSegment::Initialize(art, data.position);
	for (idx_t i = 0; i < count_p; i++) {
		segment = segment->Append(art, count, reader.Read<uint8_t>());
	}
	D_ASSERT(count_p == count);
}

bool Prefix::IsInlined() const {
	return count <= ARTNode::PREFIX_INLINE_BYTES;
}

void Prefix::MoveInlinedToSegment(ART &art) {
	D_ASSERT(IsInlined());
	auto position = PrefixSegment::New(art);
	auto segment = PrefixSegment::Initialize(art, position);

	// move data
	D_ASSERT(ARTNode::PREFIX_SEGMENT_SIZE >= ARTNode::PREFIX_INLINE_BYTES);
	memmove(segment->bytes, data.inlined, count);
	data.position = position;
}

} // namespace duckdb
