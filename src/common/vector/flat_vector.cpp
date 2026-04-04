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
	// Preserve the validity mask from the old buffer before replacing it.
	// FIXME: this can maybe be removed in the future - it seems only the Arrow conversion code relies on this behavior
	auto old_validity = std::move(vector.buffer->GetValidityMask());
	if (vector.GetType().InternalType() == PhysicalType::LIST) {
		auto &current_buffer = vector.buffer->Cast<VectorListBuffer>();
		vector.buffer = make_buffer<VectorListBuffer>(data, current_buffer.GetChild(), current_buffer.GetCapacity(),
		                                              current_buffer.GetSize());
	} else if (vector.GetType().InternalType() == PhysicalType::VARCHAR) {
		vector.buffer = make_buffer<VectorStringBuffer>(data);
	} else {
		vector.buffer = make_buffer<StandardVectorBuffer>(data);
	}
	vector.buffer->GetValidityMask() = std::move(old_validity);
}

void FlatVector::SetNull(Vector &vector, idx_t idx, bool is_null) {
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
	vector.buffer->GetValidityMask().Set(idx, !is_null);
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
