#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"

namespace duckdb {

string_t &FlatVector::StringElement::EmptyString(idx_t length) {
	auto &heap = writer.GetHeap();
	data[idx] = heap.EmptyString(length);
	return data[idx];
}

string_t &FlatVector::StringElement::operator=(string_t val) {
	auto &heap = writer.GetHeap();
	data[idx] = heap.AddBlob(val);
	return data[idx];
}

StringHeap &FlatVector::FlatStringWriter::GetHeap() {
	if (!heap) {
		heap = StringVector::GetStringHeap(vector);
	}
	return *heap;
}

VectorStringBuffer::VectorStringBuffer() : StandardVectorBuffer(idx_t(0)) {
	buffer_type = VectorBufferType::STRING_BUFFER;
}

VectorStringBuffer::VectorStringBuffer(Allocator &allocator)
    : StandardVectorBuffer(allocator, 0), heap(AllocateHeap(allocator)) {
	buffer_type = VectorBufferType::STRING_BUFFER;
}

VectorStringBuffer::VectorStringBuffer(Allocator &allocator, idx_t capacity)
    : StandardVectorBuffer(allocator, capacity * sizeof(string_t)) {
	buffer_type = VectorBufferType::STRING_BUFFER;
}

VectorStringBuffer::VectorStringBuffer(idx_t capacity) : StandardVectorBuffer(capacity * sizeof(string_t)) {
	buffer_type = VectorBufferType::STRING_BUFFER;
}

VectorStringBuffer::VectorStringBuffer(data_ptr_t data_ptr_p) : StandardVectorBuffer(data_ptr_p) {
	buffer_type = VectorBufferType::STRING_BUFFER;
}

VectorStringBuffer::VectorStringBuffer(AllocatedData &&data_p)
    : StandardVectorBuffer(std::move(data_p)), heap(AllocateHeap()) {
	buffer_type = VectorBufferType::STRING_BUFFER;
}

VectorStringBuffer::VectorStringBuffer(AllocatedData &&data_p, VectorStringBuffer &other)
    : StandardVectorBuffer(std::move(data_p)) {
	auto auxiliary_data = other.GetAuxiliaryData();
	if (auxiliary_data) {
		AddAuxiliaryData(make_uniq<AuxiliaryDataSetHolder>(std::move(auxiliary_data)));
	}
	buffer_type = VectorBufferType::STRING_BUFFER;
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
	if (!vector.buffer) {
		vector.buffer = make_buffer<VectorStringBuffer>(nullptr);
	}
	if (vector.buffer->GetBufferType() != VectorBufferType::STRING_BUFFER) {
		throw InternalException(
		    "StringVector::GetStringBuffer called on a vector - but that vector does NOT have a string buffer");
	}
	return vector.buffer->Cast<VectorStringBuffer>();
}

ArenaAllocator &StringVector::GetStringAllocator(Vector &vector) {
	return GetStringBuffer(vector).GetStringAllocator();
}

StringHeap &StringVector::GetStringHeap(Vector &vector) {
	auto &string_buffer = GetStringBuffer(vector);
	return string_buffer.GetHeap();
}

string_t StringVector::AddString(Vector &vector, string_t data) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::VARCHAR || vector.GetType().id() == LogicalTypeId::BIT);
	auto &string_heap = GetStringHeap(vector);
	return string_heap.AddString(data);
}

string_t StringVector::AddStringOrBlob(Vector &vector, string_t data) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	return GetStringHeap(vector).AddBlob(data);
}

string_t StringVector::EmptyString(Vector &vector, idx_t len) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	auto &string_heap = GetStringHeap(vector);
	return string_heap.EmptyString(len);
}

void StringVector::AddAuxiliaryData(Vector &vector, unique_ptr<AuxiliaryDataHolder> data) {
	vector.AddAuxiliaryData(std::move(data));
}

void StringVector::AddHandle(Vector &vector, BufferHandle handle) {
	AddAuxiliaryData(vector, make_uniq<PinnedBufferHolder>(std::move(handle)));
}

void StringVector::AddHeapReference(Vector &vector, const Vector &other) {
	vector.AddHeapReference(other);
}

} // namespace duckdb
