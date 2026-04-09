#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/array_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"

namespace duckdb {

StandardVectorBuffer::StandardVectorBuffer(Allocator &allocator, idx_t capacity, idx_t type_size)
    : VectorBuffer(VectorType::FLAT_VECTOR, VectorBufferType::STANDARD_BUFFER), data_ptr(nullptr) {
	if (capacity > 0) {
		allocated_data = allocator.Allocate(capacity * type_size);
		data_ptr = allocated_data.get();
		// resize the validity
		validity.Resize(capacity);
	}
}
StandardVectorBuffer::StandardVectorBuffer(idx_t capacity, idx_t type_size)
    : StandardVectorBuffer(Allocator::DefaultAllocator(), capacity, type_size) {
}
StandardVectorBuffer::StandardVectorBuffer(data_ptr_t data_ptr_p)
    : VectorBuffer(VectorType::FLAT_VECTOR, VectorBufferType::STANDARD_BUFFER), data_ptr(data_ptr_p) {
}
StandardVectorBuffer::StandardVectorBuffer(AllocatedData &&data_p)
    : VectorBuffer(VectorType::FLAT_VECTOR, VectorBufferType::STANDARD_BUFFER), data_ptr(data_p.get()),
      allocated_data(std::move(data_p)) {
}

void StandardVectorBuffer::SetVectorType(VectorType new_vector_type) {
	vector_type = new_vector_type;
}

idx_t StandardVectorBuffer::GetAllocationSize() const {
	idx_t size = VectorBuffer::GetAllocationSize();
	size += allocated_data.GetSize();
	size += validity.GetAllocationSize();
	return size;
}

void StandardVectorBuffer::Verify(const LogicalType &type, const SelectionVector &sel, idx_t count) const {
	D_ASSERT(vector_type == VectorType::FLAT_VECTOR || vector_type == VectorType::CONSTANT_VECTOR);
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		return;
	}
	// verify all entries in the sel fit within the validity
	if (sel.IsSet()) {
		for (idx_t i = 0; i < count; i++) {
			D_ASSERT(sel.get_index(i) < validity.Capacity());
		}
	} else {
		D_ASSERT(count <= validity.Capacity());
	}
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
