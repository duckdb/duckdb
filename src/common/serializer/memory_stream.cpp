#include "duckdb/common/serializer/memory_stream.hpp"

#include "duckdb/common/allocator.hpp"

namespace duckdb {

MemoryStream::MemoryStream(Allocator &allocator_p, idx_t capacity)
    : allocator(&allocator_p), position(0), capacity(capacity) {
	D_ASSERT(capacity != 0 && IsPowerOfTwo(capacity));
	data = allocator_p.AllocateData(capacity);
}

MemoryStream::MemoryStream(idx_t capacity) : MemoryStream(Allocator::DefaultAllocator(), capacity) {
}

MemoryStream::MemoryStream(data_ptr_t buffer, idx_t capacity) : position(0), capacity(capacity), data(buffer) {
}

MemoryStream::~MemoryStream() {
	if (allocator && data) {
		allocator->FreeData(data, capacity);
	}
}

MemoryStream::MemoryStream(MemoryStream &&other) noexcept {
	// Move the data from the other stream into this stream
	data = other.data;
	position = other.position;
	capacity = other.capacity;
	allocator = other.allocator;

	// Reset the other stream
	other.data = nullptr;
	other.position = 0;
	other.capacity = 0;
	other.allocator = nullptr;
}

MemoryStream &MemoryStream::operator=(MemoryStream &&other) noexcept {
	if (this != &other) {
		// Free the current data
		if (allocator) {
			allocator->FreeData(data, capacity);
		}

		// Move the data from the other stream into this stream
		data = other.data;
		position = other.position;
		capacity = other.capacity;
		allocator = other.allocator;

		// Reset the other stream
		other.data = nullptr;
		other.position = 0;
		other.capacity = 0;
		other.allocator = nullptr;
	}
	return *this;
}

void MemoryStream::WriteData(const_data_ptr_t source, idx_t write_size) {
	const auto old_capacity = capacity;
	while (position + write_size > capacity) {
		if (allocator) {
			capacity *= 2;
		} else {
			throw SerializationException("Failed to serialize: not enough space in buffer to fulfill write request");
		}
	}
	if (capacity != old_capacity) {
		data = allocator->ReallocateData(data, old_capacity, capacity);
	}
	memcpy(data + position, source, write_size);
	position += write_size;
}

void MemoryStream::ReadData(data_ptr_t destination, idx_t read_size) {
	if (position + read_size > capacity) {
		throw SerializationException("Failed to deserialize: not enough data in buffer to fulfill read request");
	}
	memcpy(destination, data + position, read_size);
	position += read_size;
}

void MemoryStream::Rewind() {
	position = 0;
}

void MemoryStream::Release() {
	allocator = nullptr;
}

data_ptr_t MemoryStream::GetData() const {
	return data;
}

idx_t MemoryStream::GetPosition() const {
	return position;
}

idx_t MemoryStream::GetCapacity() const {
	return capacity;
}

void MemoryStream::SetPosition(idx_t position_p) {
	position = position_p;
}

} // namespace duckdb
