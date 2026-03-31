#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"

namespace duckdb {

VectorStringBuffer::VectorStringBuffer() : StandardVectorBuffer(idx_t(0)), heap(AllocateHeap()) {
	buffer_type = VectorBufferType::STRING_BUFFER;
}

VectorStringBuffer::VectorStringBuffer(Allocator &allocator)
    : StandardVectorBuffer(allocator, 0), heap(AllocateHeap(allocator)) {
	buffer_type = VectorBufferType::STRING_BUFFER;
}

VectorStringBuffer::VectorStringBuffer(Allocator &allocator, idx_t data_size)
    : StandardVectorBuffer(allocator, data_size), heap(AllocateHeap(allocator)) {
	buffer_type = VectorBufferType::STRING_BUFFER;
}

VectorStringBuffer::VectorStringBuffer(idx_t data_size) : StandardVectorBuffer(data_size), heap(AllocateHeap()) {
	buffer_type = VectorBufferType::STRING_BUFFER;
}

VectorStringBuffer::VectorStringBuffer(data_ptr_t data_ptr_p) : StandardVectorBuffer(data_ptr_p), heap(AllocateHeap()) {
	buffer_type = VectorBufferType::STRING_BUFFER;
}

VectorStringBuffer::VectorStringBuffer(AllocatedData &&data_p)
    : StandardVectorBuffer(std::move(data_p)), heap(AllocateHeap()) {
	buffer_type = VectorBufferType::STRING_BUFFER;
}

VectorStringBuffer::VectorStringBuffer(VectorBufferType type) : StandardVectorBuffer(idx_t(0)), heap(AllocateHeap()) {
	buffer_type = type;
}

VectorStringBuffer::VectorStringBuffer(AllocatedData &&data_p, const VectorStringBuffer &other)
    : StandardVectorBuffer(std::move(data_p)), heap(AllocateHeap()) {
	auxiliary_data = other.auxiliary_data;
}

StringHeap &VectorStringBuffer::AllocateHeap(Allocator &allocator) {
	auto data = make_uniq<StringHeapHolder>(allocator);
	auto &result = data->heap;
	AddAuxiliaryData(std::move(data));
	return result;
}

StringHeap &VectorStringBuffer::AllocateHeap() {
	return AllocateHeap(Allocator::DefaultAllocator());
}

string_t StringVector::AddString(Vector &vector, const char *data, idx_t len) {
	return StringVector::AddString(vector, string_t(data, UnsafeNumericCast<uint32_t>(len)));
}

string_t StringVector::AddStringOrBlob(Vector &vector, const char *data, idx_t len) {
	return StringVector::AddStringOrBlob(vector, string_t(data, UnsafeNumericCast<uint32_t>(len)));
}

string_t StringVector::AddString(Vector &vector, const char *data) {
	return StringVector::AddString(vector, string_t(data, UnsafeNumericCast<uint32_t>(strlen(data))));
}

string_t StringVector::AddString(Vector &vector, const string &data) {
	return StringVector::AddString(vector, string_t(data.c_str(), UnsafeNumericCast<uint32_t>(data.size())));
}

VectorStringBuffer &StringVector::GetStringBuffer(Vector &vector) {
	if (vector.GetType().InternalType() != PhysicalType::VARCHAR) {
		throw InternalException("StringVector::GetStringBuffer - vector is not of internal type VARCHAR but of type %s",
		                        vector.GetType());
	}
	// check if the main buffer is a VectorStringBuffer
	if (vector.buffer && vector.buffer->GetBufferType() == VectorBufferType::STRING_BUFFER) {
		return vector.buffer->Cast<VectorStringBuffer>();
	}
	// fall back to auxiliary (e.g. for cached vectors or vectors with external data pointers)
	if (!vector.auxiliary) {
		auto stored_allocator = vector.buffer ? vector.buffer->GetAllocator() : nullptr;
		if (stored_allocator) {
			vector.auxiliary = make_buffer<VectorStringBuffer>(*stored_allocator);
		} else {
			vector.auxiliary = make_buffer<VectorStringBuffer>();
		}
	}
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::STRING_BUFFER);
	return vector.auxiliary.get()->Cast<VectorStringBuffer>();
}

ArenaAllocator &StringVector::GetStringAllocator(Vector &vector) {
	return GetStringBuffer(vector).GetStringAllocator();
}

string_t StringVector::AddString(Vector &vector, string_t data) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::VARCHAR || vector.GetType().id() == LogicalTypeId::BIT);
	if (data.IsInlined()) {
		// string will be inlined: no need to store in string heap
		return data;
	}
	auto &string_buffer = GetStringBuffer(vector);
	return string_buffer.AddString(data);
}

string_t StringVector::AddStringOrBlob(Vector &vector, string_t data) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	if (data.IsInlined()) {
		// string will be inlined: no need to store in string heap
		return data;
	}
	auto &string_buffer = GetStringBuffer(vector);
	return string_buffer.AddBlob(data);
}

string_t StringVector::EmptyString(Vector &vector, idx_t len) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	if (len <= string_t::INLINE_LENGTH) {
		return string_t(UnsafeNumericCast<uint32_t>(len));
	}
	auto &string_buffer = GetStringBuffer(vector);
	return string_buffer.EmptyString(len);
}

void StringVector::AddAuxiliaryData(Vector &vector, unique_ptr<AuxiliaryDataHolder> data) {
	vector.buffer->AddAuxiliaryData(std::move(data));
}

void StringVector::AddHandle(Vector &vector, BufferHandle handle) {
	AddAuxiliaryData(vector, make_uniq<PinnedBufferHolder>(std::move(handle)));
}

void StringVector::AddHeapReference(Vector &vector, const Vector &other) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	D_ASSERT(other.GetType().InternalType() == PhysicalType::VARCHAR);

	if (other.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		AddHeapReference(vector, DictionaryVector::Child(other));
		return;
	}
	if (!other.buffer) {
		return;
	}
	auto data = other.buffer->GetAuxiliaryData();
	if (!data) {
		// no auxiliary data to reference
		return;
	}
	AddAuxiliaryData(vector, make_uniq<AuxiliaryDataSetHolder>(std::move(data)));
}

} // namespace duckdb
