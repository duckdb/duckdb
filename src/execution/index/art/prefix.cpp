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
		return Initialize();
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

void Prefix::Initialize(ART &art, const Key &key, const uint32_t &depth, const uint32_t &count_p) {

	// prefix can be inlined
	if (count_p <= ARTNode::PREFIX_INLINE_BYTES) {
		memcpy(data.inlined, key.data + depth, count_p);
		count = count_p;
		return;
	}

	// prefix cannot be inlined, copy to segment(s)
	count = 0;
	PrefixSegment::New(art, data.position);
	auto segment = PrefixSegment::Initialize(art, data.position);
	for (idx_t i = 0; i < count_p; i++) {
		segment = segment->Append(art, count, key.data[depth + i]);
	}
	D_ASSERT(count == count_p);

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
	PrefixSegment::New(art, data.position);
	auto segment = PrefixSegment::Initialize(art, data.position);

	// iterate the segments of the other prefix and copy their data
	auto other_position = other.data.position;
	auto remaining = count_p;

	while (remaining != 0) {
		D_ASSERT(other_position != DConstants::INVALID_INDEX);
		auto other_segment = PrefixSegment::Get(art, other_position);
		auto copy_count = MinValue(ARTNode::PREFIX_SEGMENT_SIZE, remaining);

		// copy the data
		for (idx_t i = 0; i < copy_count; i++) {
			segment = segment->Append(art, count, other_segment->bytes[i]);
		}

		// adjust the loop variables
		other_position = other_segment->next;
		remaining -= copy_count;
	}
	D_ASSERT(count == count_p);
}

void Prefix::Vacuum(ART &art) {

	if (IsInlined()) {
		return;
	}

	// first position has special treatment because we don't obtain it from a prefix segment
	auto allocator = art.GetAllocator(ARTNodeType::PREFIX_SEGMENT);
	if (allocator->NeedsVacuum(data.position)) {
		allocator->Vacuum(data.position);
	}

	auto position = data.position;
	while (position != DConstants::INVALID_INDEX) {
		auto segment = PrefixSegment::Get(art, position);
		if (segment->next != DConstants::INVALID_INDEX && allocator->NeedsVacuum(segment->next)) {
			allocator->Vacuum(segment->next);
		}
		position = segment->next;
	}
}

void Prefix::InitializeMerge(ART &art, const idx_t &buffer_count) {

	if (IsInlined()) {
		return;
	}

	auto segment = PrefixSegment::Get(art, data.position);
	D_ASSERT((data.position & FixedSizeAllocator::BUFFER_ID_TO_ZERO) ==
	         ((data.position + buffer_count) & FixedSizeAllocator::BUFFER_ID_TO_ZERO));
	data.position += buffer_count;

	auto position = segment->next;
	while (position != DConstants::INVALID_INDEX) {
		D_ASSERT((segment->next & FixedSizeAllocator::BUFFER_ID_TO_ZERO) ==
		         ((segment->next + buffer_count) & FixedSizeAllocator::BUFFER_ID_TO_ZERO));
		segment->next += buffer_count;
		segment = PrefixSegment::Get(art, position);
		position = segment->next;
	}
}

void Prefix::Append(ART &art, const Prefix &other) {

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
	auto other_position = other.data.position;
	auto remaining = other.count;

	while (other_position != DConstants::INVALID_INDEX) {
		auto other_segment = PrefixSegment::Get(art, other_position);
		auto copy_count = MinValue(ARTNode::PREFIX_SEGMENT_SIZE, remaining);

		// copy the data
		for (idx_t i = 0; i < copy_count; i++) {
			segment = segment->Append(art, count, other_segment->bytes[i]);
		}

		// adjust the loop variables
		other_position = other_segment->next;
		remaining -= copy_count;
	}
	D_ASSERT(remaining == 0);
}

void Prefix::Concatenate(ART &art, const uint8_t &byte, const Prefix &other) {

	auto new_size = count + 1 + other.count;

	// overwrite into this prefix (both are inlined)
	if (new_size <= ARTNode::PREFIX_INLINE_BYTES) {
		// move this prefix backwards
		memmove(data.inlined + other.count + 1, data.inlined, count);
		// copy byte
		data.inlined[other.count] = byte;
		// copy the other prefix into this prefix
		memcpy(data.inlined, other.data.inlined, other.count);
		count = new_size;
		return;
	}

	// else, create a new prefix and then move it into the existing prefix
	Prefix new_prefix;
	// append the other prefix
	new_prefix.Append(art, other);
	// append the byte
	// FIXME: this is a bit hacky, a SetByte function would probably be cleaner
	Prefix byte_prefix(byte);
	new_prefix.Append(art, byte_prefix);
	// append this prefix
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
		memmove(data.inlined, data.inlined + n + 1, new_count);
		count = new_count;
		return new_first_byte;
	}

	count = 0;
	auto start = n + 1;
	auto offset = start % ARTNode::PREFIX_SEGMENT_SIZE;
	auto remaining = new_count;

	// get the source segment, i.e., the segment that contains the byte at start
	auto src_segment = PrefixSegment::Get(art, data.position);
	for (idx_t i = 0; i < start / ARTNode::PREFIX_SEGMENT_SIZE; i++) {
		D_ASSERT(src_segment->next != DConstants::INVALID_INDEX);
		src_segment = PrefixSegment::Get(art, src_segment->next);
	}

	// iterate all segments starting at the source segment and copy, i.e. shift, their data
	auto dst_segment = PrefixSegment::Get(art, data.position);
	while (true) {
		auto copy_count = MinValue(ARTNode::PREFIX_SEGMENT_SIZE - offset, remaining);

		// copy the data
		for (idx_t i = offset; i < offset + copy_count; i++) {
			dst_segment = dst_segment->Append(art, count, src_segment->bytes[i]);
		}

		// adjust the loop variables
		offset = 0;
		remaining -= copy_count;
		if (remaining == 0) {
			break;
		}
		D_ASSERT(src_segment->next != DConstants::INVALID_INDEX);
		src_segment = PrefixSegment::Get(art, src_segment->next);
	}

	// possibly inline the data
	if (IsInlined()) {
		MoveSegmentToInlined(art);
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

	if (IsInlined()) {
		for (idx_t mismatch_position = 0; mismatch_position < count; mismatch_position++) {
			D_ASSERT(depth + mismatch_position < key.len);
			if (key[depth + mismatch_position] != data.inlined[mismatch_position]) {
				return mismatch_position;
			}
		}
		return count;
	}

	uint32_t mismatch_position = 0;
	auto position = data.position;

	while (mismatch_position != count) {
		D_ASSERT(depth + mismatch_position < key.len);
		D_ASSERT(position != DConstants::INVALID_INDEX);

		auto segment = PrefixSegment::Get(art, position);
		auto compare_count = MinValue(ARTNode::PREFIX_SEGMENT_SIZE, count - mismatch_position);

		// compare bytes
		for (uint32_t i = 0; i < compare_count; i++) {
			if (key[depth + mismatch_position] != segment->bytes[i]) {
				return mismatch_position;
			}
			mismatch_position++;
		}

		// adjust loop variables
		position = segment->next;
	}
	return count;
}

uint32_t Prefix::MismatchPosition(ART &art, const Prefix &other) const {

	D_ASSERT(count <= other.count);

	// case 1: both prefixes are inlined
	if (IsInlined() && other.IsInlined()) {
		for (uint32_t i = 0; i < count; i++) {
			if (data.inlined[i] != other.data.inlined[i]) {
				return i;
			}
		}
		return count;
	}

	// case 2: only this prefix is inlined
	if (IsInlined()) {
		// we only need the first segment of the other prefix
		auto segment = PrefixSegment::Get(art, other.data.position);
		for (uint32_t i = 0; i < count; i++) {
			if (data.inlined[i] != segment->bytes[i]) {
				return i;
			}
		}
		return count;
	}

	// case 3: both prefixes are not inlined
	auto position = data.position;
	auto other_position = other.data.position;

	// iterate segments and compare bytes
	uint32_t mismatch_position = 0;
	while (position != DConstants::INVALID_INDEX) {
		D_ASSERT(other_position != DConstants::INVALID_INDEX);
		auto segment = PrefixSegment::Get(art, position);
		auto other_segment = PrefixSegment::Get(art, other_position);

		// compare bytes
		auto compare_count = MinValue(ARTNode::PREFIX_SEGMENT_SIZE, count - mismatch_position);
		for (uint32_t i = 0; i < compare_count; i++) {
			if (segment->bytes[i] != other_segment->bytes[i]) {
				return mismatch_position;
			}
			mismatch_position++;
		}

		// adjust loop variables
		position = segment->next;
		other_position = other_segment->next;
	}
	return count;
}

void Prefix::Serialize(ART &art, MetaBlockWriter &writer) const {

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
		auto copy_count = MinValue(ARTNode::PREFIX_SEGMENT_SIZE, remaining);

		// write the bytes
		writer.WriteData(segment->bytes, copy_count);

		// adjust loop variables
		remaining -= copy_count;
		position = segment->next;
	}
	D_ASSERT(remaining == 0);
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
	PrefixSegment::New(art, data.position);
	auto segment = PrefixSegment::Initialize(art, data.position);
	for (idx_t i = 0; i < count_p; i++) {
		segment = segment->Append(art, count, reader.Read<uint8_t>());
	}
	D_ASSERT(count_p == count);
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

void Prefix::MoveSegmentToInlined(ART &art) {

	D_ASSERT(IsInlined());
	D_ASSERT(data.position != DConstants::INVALID_INDEX);

	auto position = data.position;
	auto segment = PrefixSegment::Get(art, data.position);

	// move data
	memmove(data.inlined, segment->bytes, count);

	// free segment
	PrefixSegment::Free(art, position);
}

} // namespace duckdb
