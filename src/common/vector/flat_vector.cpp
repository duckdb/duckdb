#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/array_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"

namespace duckdb {

StandardVectorBuffer::StandardVectorBuffer(Allocator &allocator, idx_t data_size)
    : VectorBuffer(VectorBufferType::STANDARD_BUFFER), data_ptr(nullptr) {
	if (data_size > 0) {
		allocated_data = allocator.Allocate(data_size);
		data_ptr = allocated_data.get();
	}
}
StandardVectorBuffer::StandardVectorBuffer(idx_t data_size)
    : StandardVectorBuffer(Allocator::DefaultAllocator(), data_size) {
}
StandardVectorBuffer::StandardVectorBuffer(data_ptr_t data_ptr_p)
    : VectorBuffer(VectorBufferType::STANDARD_BUFFER), data_ptr(data_ptr_p) {
}
StandardVectorBuffer::StandardVectorBuffer(AllocatedData &&data_p)
    : VectorBuffer(VectorBufferType::STANDARD_BUFFER), data_ptr(data_p.get()), allocated_data(std::move(data_p)) {
}

void FlatVector::SetData(Vector &vector, data_ptr_t data) {
	VerifyFlatVector(vector);
	if (vector.GetType().InternalType() == PhysicalType::ARRAY) {
		throw InternalException("SetData not supported for array");
	}
	if (vector.GetType().InternalType() == PhysicalType::STRUCT) {
		throw InternalException("SetData not supported for struct");
	}
	if (vector.GetType().InternalType() == PhysicalType::LIST) {
		auto &current_buffer = vector.buffer->Cast<VectorListBuffer>();
		vector.buffer = make_buffer<VectorListBuffer>(data, current_buffer.GetChild(), current_buffer.GetCapacity(),
		                                              current_buffer.GetSize());
		return;
	}
	if (vector.GetType().InternalType() == PhysicalType::VARCHAR) {
		vector.buffer = make_buffer<VectorStringBuffer>(data);
		return;
	}
	vector.buffer = make_buffer<StandardVectorBuffer>(data);
}

void FlatVector::SetNull(Vector &vector, idx_t idx, bool is_null) {
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
	vector.validity.Set(idx, !is_null);
	if (!is_null) {
		return;
	}

	auto &type = vector.GetType();
	auto internal_type = type.InternalType();

	// Set all child entries to NULL.
	if (internal_type == PhysicalType::STRUCT) {
		auto &entries = StructVector::GetEntries(vector);
		for (auto &entry : entries) {
			FlatVector::SetNull(entry, idx, is_null);
		}
		return;
	}

	// Set all child entries to NULL.
	if (internal_type == PhysicalType::ARRAY) {
		auto &child = ArrayVector::GetEntry(vector);
		auto array_size = ArrayType::GetSize(type);
		auto child_offset = idx * array_size;
		for (idx_t i = 0; i < array_size; i++) {
			FlatVector::SetNull(child, child_offset + i, is_null);
		}
	}
}

} // namespace duckdb
