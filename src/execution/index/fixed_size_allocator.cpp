#include "duckdb/execution/index/fixed_size_allocator.hpp"

#include "duckdb/storage/metadata/metadata_reader.hpp"

namespace duckdb {

FixedSizeAllocator::FixedSizeAllocator(const idx_t segment_size, BlockManager &block_manager)
    : block_manager(block_manager), buffer_manager(block_manager.buffer_manager), segment_size(segment_size),
      total_segment_count(0) {

	if (segment_size > block_manager.GetBlockSize() - sizeof(validity_t)) {
		throw InternalException("The maximum segment size of fixed-size allocators is " +
		                        to_string(block_manager.GetBlockSize() - sizeof(validity_t)));
	}

	// calculate how many segments fit into one buffer (available_segments_per_buffer)

	idx_t bits_per_value = sizeof(validity_t) * 8;
	idx_t byte_count = 0;

	bitmask_count = 0;
	available_segments_per_buffer = 0;

	while (byte_count < block_manager.GetBlockSize()) {
		if (!bitmask_count || (bitmask_count * bits_per_value) % available_segments_per_buffer == 0) {
			// we need to add another validity_t value to the bitmask, to allow storing another
			// bits_per_value segments on a buffer
			bitmask_count++;
			byte_count += sizeof(validity_t);
		}

		auto remaining_bytes = block_manager.GetBlockSize() - byte_count;
		auto remaining_segments = MinValue(remaining_bytes / segment_size, bits_per_value);

		if (remaining_segments == 0) {
			break;
		}

		available_segments_per_buffer += remaining_segments;
		byte_count += remaining_segments * segment_size;
	}

	bitmask_offset = bitmask_count * sizeof(validity_t);
}

IndexPointer FixedSizeAllocator::New() {
	// no more segments available
	if (buffers_with_free_space.empty()) {

		// add a new buffer
		auto buffer_id = GetAvailableBufferId();
		buffers[buffer_id] = make_uniq<FixedSizeBuffer>(block_manager);
		buffers_with_free_space.insert(buffer_id);

		// set the bitmask
		D_ASSERT(buffers.find(buffer_id) != buffers.end());
		auto &buffer = buffers.find(buffer_id)->second;
		ValidityMask mask(reinterpret_cast<validity_t *>(buffer->Get()), available_segments_per_buffer);

		// zero-initialize the bitmask to avoid leaking memory to disk
		auto data = mask.GetData();
		for (idx_t i = 0; i < bitmask_count; i++) {
			data[i] = 0;
		}

		// initializing the bitmask of the new buffer
		mask.SetAllValid(available_segments_per_buffer);
	}

	// return a pointer to a free segment
	D_ASSERT(!buffers_with_free_space.empty());
	auto buffer_id = uint32_t(*buffers_with_free_space.begin());

	D_ASSERT(buffers.find(buffer_id) != buffers.end());
	auto &buffer = buffers.find(buffer_id)->second;
	auto offset = buffer->GetOffset(bitmask_count, available_segments_per_buffer);

	total_segment_count++;
	buffer->segment_count++;
	if (buffer->segment_count == available_segments_per_buffer) {
		buffers_with_free_space.erase(buffer_id);
	}

	// zero-initialize that segment
	auto buffer_ptr = buffer->Get();
	auto offset_in_buffer = buffer_ptr + offset * segment_size + bitmask_offset;
	memset(offset_in_buffer, 0, segment_size);

	return IndexPointer(buffer_id, offset);
}

void FixedSizeAllocator::Free(const IndexPointer ptr) {

	auto buffer_id = ptr.GetBufferId();
	auto offset = ptr.GetOffset();

	D_ASSERT(buffers.find(buffer_id) != buffers.end());
	auto &buffer = buffers.find(buffer_id)->second;

	auto bitmask_ptr = reinterpret_cast<validity_t *>(buffer->Get());
	ValidityMask mask(bitmask_ptr, offset + 1); // FIXME
	D_ASSERT(!mask.RowIsValid(offset));
	mask.SetValid(offset);

	D_ASSERT(total_segment_count > 0);
	D_ASSERT(buffer->segment_count > 0);

	// adjust the allocator fields
	buffers_with_free_space.insert(buffer_id);
	total_segment_count--;
	buffer->segment_count--;
}

void FixedSizeAllocator::Reset() {
	buffers.clear();
	buffers_with_free_space.clear();
	total_segment_count = 0;
}

idx_t FixedSizeAllocator::GetInMemorySize() const {
	idx_t memory_usage = 0;
	for (auto &buffer : buffers) {
		if (buffer.second->InMemory()) {
			memory_usage += block_manager.GetBlockSize();
		}
	}
	return memory_usage;
}

idx_t FixedSizeAllocator::GetUpperBoundBufferId() const {
	idx_t upper_bound_id = 0;
	for (auto &buffer : buffers) {
		if (buffer.first >= upper_bound_id) {
			upper_bound_id = buffer.first + 1;
		}
	}
	return upper_bound_id;
}

void FixedSizeAllocator::Merge(FixedSizeAllocator &other) {

	D_ASSERT(segment_size == other.segment_size);

	// remember the buffer count and merge the buffers
	idx_t upper_bound_id = GetUpperBoundBufferId();
	for (auto &buffer : other.buffers) {
		buffers.insert(make_pair(buffer.first + upper_bound_id, std::move(buffer.second)));
	}
	other.buffers.clear();

	// merge the buffers with free spaces
	for (auto &buffer_id : other.buffers_with_free_space) {
		buffers_with_free_space.insert(buffer_id + upper_bound_id);
	}
	other.buffers_with_free_space.clear();

	// add the total allocations
	total_segment_count += other.total_segment_count;
}

bool FixedSizeAllocator::InitializeVacuum() {

	// NOTE: we do not vacuum buffers that are not in memory. We might consider changing this
	// in the future, although buffers on disk should almost never be eligible for a vacuum

	if (total_segment_count == 0) {
		Reset();
		return false;
	}
	RemoveEmptyBuffers();

	// determine if a vacuum is necessary
	multimap<idx_t, idx_t> temporary_vacuum_buffers;
	D_ASSERT(vacuum_buffers.empty());
	idx_t available_segments_in_memory = 0;

	for (auto &buffer : buffers) {
		buffer.second->vacuum = false;
		if (buffer.second->InMemory()) {
			auto available_segments_in_buffer = available_segments_per_buffer - buffer.second->segment_count;
			available_segments_in_memory += available_segments_in_buffer;
			temporary_vacuum_buffers.emplace(available_segments_in_buffer, buffer.first);
		}
	}

	// no buffers in memory
	if (temporary_vacuum_buffers.empty()) {
		return false;
	}

	auto excess_buffer_count = available_segments_in_memory / available_segments_per_buffer;

	// calculate the vacuum threshold adaptively
	D_ASSERT(excess_buffer_count < temporary_vacuum_buffers.size());
	idx_t memory_usage = GetInMemorySize();
	idx_t excess_memory_usage = excess_buffer_count * block_manager.GetBlockSize();
	auto excess_percentage = double(excess_memory_usage) / double(memory_usage);
	auto threshold = double(VACUUM_THRESHOLD) / 100.0;
	if (excess_percentage < threshold) {
		return false;
	}

	D_ASSERT(excess_buffer_count <= temporary_vacuum_buffers.size());
	D_ASSERT(temporary_vacuum_buffers.size() <= buffers.size());

	// erasing from a multimap, we vacuum the buffers with the most free spaces (least full)
	while (temporary_vacuum_buffers.size() != excess_buffer_count) {
		temporary_vacuum_buffers.erase(temporary_vacuum_buffers.begin());
	}

	// adjust the buffers, and erase all to-be-vacuumed buffers from the available buffer list
	for (auto &vacuum_buffer : temporary_vacuum_buffers) {
		auto buffer_id = vacuum_buffer.second;
		D_ASSERT(buffers.find(buffer_id) != buffers.end());
		buffers.find(buffer_id)->second->vacuum = true;
		buffers_with_free_space.erase(buffer_id);
	}

	for (auto &vacuum_buffer : temporary_vacuum_buffers) {
		vacuum_buffers.insert(vacuum_buffer.second);
	}

	return true;
}

void FixedSizeAllocator::FinalizeVacuum() {

	for (auto &buffer_id : vacuum_buffers) {
		D_ASSERT(buffers.find(buffer_id) != buffers.end());
		D_ASSERT(buffers.find(buffer_id)->second->InMemory());
		buffers.erase(buffer_id);
	}
	vacuum_buffers.clear();
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

FixedSizeAllocatorInfo FixedSizeAllocator::GetInfo() const {

	FixedSizeAllocatorInfo info;
	info.segment_size = segment_size;

	for (const auto &buffer : buffers) {
		info.buffer_ids.push_back(buffer.first);

		// Memory safety check.
		if (buffer.first > idx_t(MAX_ROW_ID)) {
			throw InternalException("Initializing invalid buffer ID in FixedSizeAllocator::GetInfo");
		}

		info.block_pointers.push_back(buffer.second->block_pointer);
		info.segment_counts.push_back(buffer.second->segment_count);
		info.allocation_sizes.push_back(buffer.second->allocation_size);
	}

	for (auto &buffer_id : buffers_with_free_space) {
		info.buffers_with_free_space.push_back(buffer_id);
	}

	return info;
}

void FixedSizeAllocator::SerializeBuffers(PartialBlockManager &partial_block_manager) {
	for (auto &buffer : buffers) {
		buffer.second->Serialize(partial_block_manager, available_segments_per_buffer, segment_size, bitmask_offset);
	}
}

vector<IndexBufferInfo> FixedSizeAllocator::InitSerializationToWAL() {

	vector<IndexBufferInfo> buffer_infos;
	for (auto &buffer : buffers) {
		buffer.second->SetAllocationSize(available_segments_per_buffer, segment_size, bitmask_offset);
		buffer_infos.emplace_back(buffer.second->Get(), buffer.second->allocation_size);
	}
	return buffer_infos;
}

void FixedSizeAllocator::Init(const FixedSizeAllocatorInfo &info) {
	segment_size = info.segment_size;
	total_segment_count = 0;

	for (idx_t i = 0; i < info.buffer_ids.size(); i++) {

		// read all FixedSizeBuffer data
		auto buffer_id = info.buffer_ids[i];

		// Memory safety check.
		if (buffer_id > idx_t(MAX_ROW_ID)) {
			throw InternalException("Initializing invalid buffer ID in FixedSizeAllocator::Init");
		}

		auto buffer_block_pointer = info.block_pointers[i];
		auto segment_count = info.segment_counts[i];
		auto allocation_size = info.allocation_sizes[i];

		// create the FixedSizeBuffer
		buffers[buffer_id] =
		    make_uniq<FixedSizeBuffer>(block_manager, segment_count, allocation_size, buffer_block_pointer);
		total_segment_count += segment_count;
	}

	for (const auto &buffer_id : info.buffers_with_free_space) {
		buffers_with_free_space.insert(buffer_id);
	}
}

void FixedSizeAllocator::Deserialize(MetadataManager &metadata_manager, const BlockPointer &block_pointer) {

	MetadataReader reader(metadata_manager, block_pointer);
	segment_size = reader.Read<idx_t>();
	auto buffer_count = reader.Read<idx_t>();
	auto buffers_with_free_space_count = reader.Read<idx_t>();

	total_segment_count = 0;

	for (idx_t i = 0; i < buffer_count; i++) {
		auto buffer_id = reader.Read<idx_t>();
		auto buffer_block_pointer = reader.Read<BlockPointer>();
		auto segment_count = reader.Read<idx_t>();
		auto allocation_size = reader.Read<idx_t>();
		buffers[buffer_id] =
		    make_uniq<FixedSizeBuffer>(block_manager, segment_count, allocation_size, buffer_block_pointer);
		total_segment_count += segment_count;
	}
	for (idx_t i = 0; i < buffers_with_free_space_count; i++) {
		buffers_with_free_space.insert(reader.Read<idx_t>());
	}
}

idx_t FixedSizeAllocator::GetAvailableBufferId() const {
	idx_t buffer_id = buffers.size();
	while (buffers.find(buffer_id) != buffers.end()) {
		D_ASSERT(buffer_id > 0);
		buffer_id--;
	}
	return buffer_id;
}

void FixedSizeAllocator::RemoveEmptyBuffers() {

	auto buffer_it = buffers.begin();
	while (buffer_it != buffers.end()) {
		if (buffer_it->second->segment_count != 0) {
			++buffer_it;
			continue;
		}

		buffers_with_free_space.erase(buffer_it->first);
		buffer_it = buffers.erase(buffer_it);
	}
}

} // namespace duckdb
