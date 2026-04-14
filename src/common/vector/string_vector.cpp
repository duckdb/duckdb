#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/types/bignum.hpp"
#include "duckdb/common/types/bit.hpp"

namespace duckdb {

FlatVector::FlatStringWriter::FlatStringWriter(Vector &vector, idx_t count)
    : vector(vector), data(GetDataMutable<string_t>(vector)), validity(Validity(vector)), count(count) {
}

void FlatVector::FlatStringWriter::InitializeHeap() {
	heap = StringVector::GetStringHeap(vector);
}

VectorStringBuffer::VectorStringBuffer() : StandardVectorBuffer(idx_t(0), sizeof(string_t)) {
	buffer_type = VectorBufferType::STRING_BUFFER;
}

VectorStringBuffer::VectorStringBuffer(Allocator &allocator)
    : StandardVectorBuffer(allocator, 0, sizeof(string_t)), heap(AllocateHeap(allocator)) {
	buffer_type = VectorBufferType::STRING_BUFFER;
}

VectorStringBuffer::VectorStringBuffer(Allocator &allocator, idx_t capacity)
    : StandardVectorBuffer(allocator, capacity, sizeof(string_t)) {
	buffer_type = VectorBufferType::STRING_BUFFER;
}

VectorStringBuffer::VectorStringBuffer(idx_t capacity) : StandardVectorBuffer(capacity, sizeof(string_t)) {
	buffer_type = VectorBufferType::STRING_BUFFER;
}

VectorStringBuffer::VectorStringBuffer(data_ptr_t data_ptr_p, idx_t capacity)
    : StandardVectorBuffer(data_ptr_p, capacity) {
	buffer_type = VectorBufferType::STRING_BUFFER;
}

VectorStringBuffer::VectorStringBuffer(AllocatedData &&data_p, idx_t capacity)
    : StandardVectorBuffer(std::move(data_p), capacity), heap(AllocateHeap()) {
	buffer_type = VectorBufferType::STRING_BUFFER;
}

VectorStringBuffer::VectorStringBuffer(AllocatedData &&data_p, idx_t capacity, const VectorStringBuffer &other)
    : StandardVectorBuffer(std::move(data_p), capacity) {
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

idx_t StringHeapHolder::GetAllocationSize() const {
	return heap.AllocationSize();
}

buffer_ptr<VectorBuffer> VectorStringBuffer::SliceInternal(const LogicalType &type, idx_t offset, idx_t end) {
	auto type_size = GetTypeIdSize(type.InternalType());
	auto offset_ptr = data_ptr + type_size * offset;
	auto result = make_buffer<VectorStringBuffer>(offset_ptr, end - offset);
	result->GetValidityMask().Slice(validity, offset, end - offset);
	// keep the heap alive
	if (auxiliary_data) {
		result->AddAuxiliaryData(make_uniq<AuxiliaryDataSetHolder>(auxiliary_data));
	}
	return result;
}

void VectorStringBuffer::SetValue(const LogicalType &type, idx_t index, const Value &val) {
	if (!val.IsNull() && val.type() != type) {
		SetValue(type, index, val.DefaultCastAs(type));
		return;
	}
	validity.Set(index, !val.IsNull());
	if (!val.IsNull()) {
		reinterpret_cast<string_t *>(data_ptr)[index] = GetHeap().AddBlob(StringValue::Get(val));
	}
}

void VectorStringBuffer::Verify(const LogicalType &type, const SelectionVector &sel, idx_t count) const {
	StandardVectorBuffer::Verify(type, sel, count);
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		count = 1;
	}
	D_ASSERT(type.InternalType() == PhysicalType::VARCHAR);
	auto data = reinterpret_cast<const string_t *>(data_ptr);
	for (idx_t i = 0; i < count; i++) {
		auto idx = vector_type == VectorType::CONSTANT_VECTOR ? 0 : sel.get_index(i);
		if (!validity.RowIsValid(idx)) {
			// NULL
			continue;
		}
		auto &str = data[idx];
		switch (type.id()) {
		case LogicalTypeId::BIT: {
			auto buf = str.GetData();
			D_ASSERT(idx_t(*buf) < 8);
			Bit::Verify(str);
			break;
		}
		case LogicalTypeId::BIGNUM:
			Bignum::Verify(static_cast<bignum_t>(str));
			break;
		case LogicalTypeId::VARCHAR:
			// verify that the string is correct unicode
			str.Verify();
			break;
		default:
			break;
		}
	}
}

buffer_ptr<VectorBuffer> VectorStringBuffer::CreateBuffer(AllocatedData &&new_data, idx_t capacity) const {
	return make_buffer<VectorStringBuffer>(std::move(new_data), capacity, *this);
}

buffer_ptr<VectorBuffer> VectorStringBuffer::Flatten(const LogicalType &type, const SelectionVector &sel,
                                                     idx_t count) const {
	auto result = StandardVectorBuffer::Flatten(type, sel, count);
	if (!result) {
		// already flat - bail
		return nullptr;
	}
	// add heap reference from source to result
	if (auxiliary_data) {
		result->AddAuxiliaryData(make_uniq<AuxiliaryDataSetHolder>(auxiliary_data));
	}
	return result;
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
		vector.buffer = make_buffer<VectorStringBuffer>(nullptr, 0);
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
