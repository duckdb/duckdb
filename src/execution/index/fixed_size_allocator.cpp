#include "duckdb/execution/index/fixed_size_allocator.hpp"

#include "duckdb/storage/metadata/metadata_reader.hpp"

namespace duckdb {

constexpr idx_t FixedSizeAllocator::BASE[];
constexpr uint8_t FixedSizeAllocator::SHIFT[];

FixedSizeAllocator::FixedSizeAllocator(const idx_t segment_size, Allocator &allocator,
                                       MetadataManager &metadata_manager)
    : allocator(allocator), metadata_manager(metadata_manager), segment_size(segment_size), total_segment_count(0) {

	auto max_segment_size = BUFFER_SIZE + sizeof(validity_t);
	if (segment_size > max_segment_size) {
		throw InternalException("The maximum segment size of fixed-size allocators is " + to_string(max_segment_size));
	}

	// calculate how many segments fit into one buffer (available_segments_per_buffer)

	idx_t bits_per_value = sizeof(validity_t) * 8;
	idx_t byte_count = 0;

	bitmask_count = 0;
	available_segments_per_buffer = 0;

	while (byte_count < BUFFER_SIZE) {
		if (!bitmask_count || (bitmask_count * bits_per_value) % available_segments_per_buffer == 0) {
			// we need to add another validity_t value to the bitmask, to allow storing another
			// bits_per_value segments on a buffer
			bitmask_count++;
			byte_count += sizeof(validity_t);
		}

		auto remaining_bytes = BUFFER_SIZE - byte_count;
		auto remaining_segments = MinValue(remaining_bytes / segment_size, bits_per_value);

		if (remaining_segments == 0) {
			break;
		}

		available_segments_per_buffer += remaining_segments;
		byte_count += remaining_segments * segment_size;
	}

	bitmask_offset = bitmask_count * sizeof(validity_t);
}

FixedSizeAllocator::~FixedSizeAllocator() {
	for (auto &buffer : buffers) {
		if (buffer.in_memory) {
			allocator.FreeData(buffer.GetPtr(*this), BUFFER_SIZE);
		}
	}
}

IndexPointer FixedSizeAllocator::New() {

	// no more segments available
	if (buffers_with_free_space.empty()) {

		// add a new buffer
		idx_t buffer_id = buffers.size();
		D_ASSERT(buffer_id <= (uint32_t)DConstants::INVALID_INDEX);
		auto memory_ptr = allocator.AllocateData(BUFFER_SIZE);
		buffers.emplace_back(0, memory_ptr);
		buffers_with_free_space.insert(buffer_id);

		// set the bitmask
		ValidityMask mask(reinterpret_cast<validity_t *>(memory_ptr));
		mask.SetAllValid(available_segments_per_buffer);
	}

	// return a pointer
	D_ASSERT(!buffers_with_free_space.empty());
	auto buffer_id = (uint32_t)*buffers_with_free_space.begin();

	auto bitmask_ptr = reinterpret_cast<validity_t *>(buffers[buffer_id].GetPtr(*this));
	ValidityMask mask(bitmask_ptr);
	auto offset = GetOffset(mask, buffers[buffer_id].segment_count);

	buffers[buffer_id].segment_count++;
	buffers[buffer_id].dirty = true;
	total_segment_count++;
	if (buffers[buffer_id].segment_count == available_segments_per_buffer) {
		buffers_with_free_space.erase(buffer_id);
	}

	return IndexPointer(buffer_id, offset);
}

void FixedSizeAllocator::Free(const IndexPointer ptr) {

	auto buffer_id = ptr.GetBufferId();
	auto offset = ptr.GetOffset();

	auto bitmask_ptr = reinterpret_cast<validity_t *>(buffers[buffer_id].GetPtr(*this));
	ValidityMask mask(bitmask_ptr);
	D_ASSERT(!mask.RowIsValid(offset));
	mask.SetValid(offset);
	buffers_with_free_space.insert(buffer_id);

	D_ASSERT(total_segment_count > 0);
	D_ASSERT(buffers[buffer_id].segment_count > 0);

	buffers[buffer_id].segment_count--;
	buffers[buffer_id].dirty = true;
	total_segment_count--;
}

void FixedSizeAllocator::Reset() {

	for (auto &buffer : buffers) {
		if (buffer.in_memory) {
			allocator.FreeData(buffer.GetPtr(*this), BUFFER_SIZE);
		}
	}
	buffers.clear();
	buffers_with_free_space.clear();
	total_segment_count = 0;
}

idx_t FixedSizeAllocator::GetMemoryUsage() const {
	idx_t memory_usage = 0;
	for (auto &buffer : buffers) {
		if (buffer.in_memory) {
			memory_usage += BUFFER_SIZE;
		}
	}
	return memory_usage;
}

void FixedSizeAllocator::Merge(FixedSizeAllocator &other) {

	D_ASSERT(segment_size == other.segment_size);

	// remember the buffer count and merge the buffers
	idx_t buffer_count = buffers.size();
	for (auto &buffer : other.buffers) {
		buffers.push_back(buffer);
	}
	other.buffers.clear();

	// merge the buffers with free spaces
	for (auto &buffer_id : other.buffers_with_free_space) {
		buffers_with_free_space.insert(buffer_id + buffer_count);
	}
	other.buffers_with_free_space.clear();

	// add the total allocations
	total_segment_count += other.total_segment_count;
}

bool FixedSizeAllocator::InitializeVacuum() {

	if (total_segment_count == 0) {
		Reset();
		return false;
	}

	vector<idx_t> in_memory_buffers;
	idx_t available_segments_in_memory = 0;

	for (idx_t i = 0; i < buffers.size(); i++) {
		if (buffers[i].in_memory) {
			in_memory_buffers.push_back(i);
			available_segments_in_memory += available_segments_per_buffer - buffers[i].segment_count;
		}
	}
	auto excess_buffer_count = available_segments_in_memory / available_segments_per_buffer;

	// calculate the vacuum threshold adaptively
	D_ASSERT(excess_buffer_count < in_memory_buffers.size());
	idx_t memory_usage = GetMemoryUsage();
	idx_t excess_memory_usage = excess_buffer_count * BUFFER_SIZE;
	auto excess_percentage = (double)excess_memory_usage / (double)memory_usage;
	auto threshold = (double)VACUUM_THRESHOLD / 100.0;
	if (excess_percentage < threshold) {
		return false;
	}

	// set up the buffers that can be vacuumed
	vacuum_buffers.clear();
	for (idx_t i = in_memory_buffers.size(); i > 0; i--) {
		vacuum_buffers.insert(i - 1);
	}

	// remove all invalid buffers from the available buffer list to ensure that we do not reuse them
	for (auto buffer_id : vacuum_buffers) {
		auto it = buffers_with_free_space.find(buffer_id);
		if (it != buffers_with_free_space.end()) {
			buffers_with_free_space.erase(it);
		}
	}

	return true;
}

void FixedSizeAllocator::FinalizeVacuum() {

	// free all (now empty) buffers
	auto buffer_it = buffers.begin();
	idx_t buffer_id = 0;
	while (buffer_it != buffers.end()) {

		auto vacuum_it = vacuum_buffers.begin();
		while (vacuum_it != vacuum_buffers.end()) {
			if (buffer_id == *vacuum_it) {
				allocator.FreeData(buffer_it->GetPtr(*this), BUFFER_SIZE);
				buffer_it = buffers.erase(buffer_it);
				vacuum_buffers.erase(vacuum_it);
				break;
			} else {
				vacuum_it++;
			}
		}

		buffer_it++;
		buffer_id++;
	}
}

IndexPointer FixedSizeAllocator::VacuumPointer(const IndexPointer ptr) {

	// we do not need to adjust the bitmask of the old buffer, because we will free the entire
	// buffer after the vacuum operation

	auto new_ptr = New();
	// new increases the allocation count, we need to counter that here
	total_segment_count--;

	memcpy(Get(new_ptr), Get(ptr), segment_size);
	return new_ptr;
}

BlockPointer FixedSizeAllocator::Serialize(MetadataWriter &writer) {

	for (auto &buffer : buffers) {
		buffer.Serialize(*this, writer);
	}

	auto block_pointer = writer.GetBlockPointer();
	writer.Write(segment_size);
	writer.Write((idx_t)buffers.size());
	writer.Write((idx_t)buffers_with_free_space.size());

	for (auto &buffer : buffers) {
		writer.Write(buffer.block_ptr);
		writer.Write(buffer.segment_count);
	}
	for (auto &buffer_id : buffers_with_free_space) {
		writer.Write(buffer_id);
	}

	return block_pointer;
}

void FixedSizeAllocator::Deserialize(const BlockPointer &block_ptr) {

	MetadataReader reader(metadata_manager, block_ptr);
	segment_size = reader.Read<idx_t>();
	auto buffer_count = reader.Read<idx_t>();
	auto buffers_with_free_space_count = reader.Read<idx_t>();

	total_segment_count = 0;

	for (idx_t i = 0; i < buffer_count; i++) {
		auto buffer_block_ptr = reader.Read<BlockPointer>();
		auto buffer_segment_count = reader.Read<idx_t>();
		buffers.emplace_back(buffer_segment_count, buffer_block_ptr);
		total_segment_count += buffer_segment_count;
	}
	for (idx_t i = 0; i < buffers_with_free_space_count; i++) {
		buffers_with_free_space.insert(reader.Read<idx_t>());
	}
}

uint32_t FixedSizeAllocator::GetOffset(ValidityMask &mask, const idx_t segment_count) {

	auto data = mask.GetData();

	// fills up a buffer sequentially before searching for free bits
	if (mask.RowIsValid(segment_count)) {
		mask.SetInvalid(segment_count);
		return segment_count;
	}

	// get an entry with free bits
	for (idx_t entry_idx = 0; entry_idx < bitmask_count; entry_idx++) {
		if (data[entry_idx] != 0) {

			// find the position of the free bit
			auto entry = data[entry_idx];
			idx_t first_valid_bit = 0;

			// this loop finds the position of the rightmost set bit in entry and stores it
			// in first_valid_bit
			for (idx_t i = 0; i < 6; i++) {
				// set the left half of the bits of this level to zero and test if the entry is still not zero
				if (entry & BASE[i]) {
					// first valid bit is in the rightmost s[i] bits
					// permanently set the left half of the bits to zero
					entry &= BASE[i];
				} else {
					// first valid bit is in the leftmost s[i] bits
					// shift by s[i] for the next iteration and add s[i] to the position of the rightmost set bit
					entry >>= SHIFT[i];
					first_valid_bit += SHIFT[i];
				}
			}
			D_ASSERT(entry);

			auto prev_bits = entry_idx * sizeof(validity_t) * 8;
			D_ASSERT(mask.RowIsValid(prev_bits + first_valid_bit));
			mask.SetInvalid(prev_bits + first_valid_bit);
			return (prev_bits + first_valid_bit);
		}
	}

	throw InternalException("Invalid bitmask for FixedSizeAllocator");
}

} // namespace duckdb
