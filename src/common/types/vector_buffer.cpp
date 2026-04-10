#include "duckdb/common/vector/array_vector.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/fsst_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/map_vector.hpp"
#include "duckdb/common/vector/shredded_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/types/vector_buffer.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"

namespace duckdb {

buffer_ptr<VectorBuffer> VectorBuffer::CreateStandardVector(PhysicalType type, idx_t capacity) {
	if (type == PhysicalType::LIST) {
		throw InternalException("VectorBuffer::CreateStandardVector requires full list type");
	}
	if (type == PhysicalType::VARCHAR) {
		return make_buffer<VectorStringBuffer>(capacity);
	}
	return make_buffer<StandardVectorBuffer>(capacity, GetTypeIdSize(type));
}

buffer_ptr<VectorBuffer> VectorBuffer::CreateConstantVector(PhysicalType type) {
	if (type == PhysicalType::LIST) {
		throw InternalException("VectorBuffer::CreateConstantVector requires full list type");
	}
	if (type == PhysicalType::VARCHAR) {
		return make_buffer<VectorStringBuffer>(1);
	}
	return make_buffer<StandardVectorBuffer>(1ULL, GetTypeIdSize(type));
}

buffer_ptr<VectorBuffer> VectorBuffer::CreateConstantVector(const LogicalType &type) {
	if (type.InternalType() == PhysicalType::LIST) {
		return make_buffer<VectorListBuffer>(1ULL, type);
	}
	return VectorBuffer::CreateConstantVector(type.InternalType());
}

buffer_ptr<VectorBuffer> VectorBuffer::CreateStandardVector(const LogicalType &type, idx_t capacity) {
	if (type.InternalType() == PhysicalType::LIST) {
		throw InternalException("VectorBuffer::CreateStandardVector not supported for list");
	}
	return VectorBuffer::CreateStandardVector(type.InternalType(), capacity);
}

idx_t VectorBuffer::GetAllocationSize() const {
	idx_t size = 0;
	if (auxiliary_data) {
		for (auto &aux_data : auxiliary_data->data) {
			size += aux_data->GetAllocationSize();
		}
	}
	return size;
}

void VectorBuffer::Verify(const LogicalType &type, const SelectionVector &sel, idx_t count) const {
}

void VectorBuffer::SetVectorType(VectorType vector_type) {
	throw InternalException("VectorBuffer does not support SetVectorType");
}

string VectorBuffer::ToString(const LogicalType &type, idx_t count) const {
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		return GetValue(type, 0).ToString();
	}
	string retval;
	for (idx_t i = 0; i < count; i++) {
		retval += GetValue(type, i).ToString();
		if (i < count - 1) {
			retval += ", ";
		}
	}
	return retval;
}

string VectorBuffer::ToString(const LogicalType &type) const {
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		return GetValue(type, 0).ToString();
	}
	return "";
}

buffer_ptr<VectorBuffer> VectorBuffer::Resize(const LogicalType &type, idx_t current_size, idx_t new_size) const {
	throw InternalException("VectorBuffer::Resize not supported for this vector type");
}

void VectorBuffer::ToUnifiedFormat(idx_t count, UnifiedVectorFormat &format) const {
	throw InternalException("ToUnifiedFormat not supported for this buffer type - flatten first");
}

buffer_ptr<VectorBuffer> VectorBuffer::Flatten(const LogicalType &type, const SelectionVector &sel, idx_t count) const {
	throw InternalException("Unimplemented type for flatten");
}

buffer_ptr<VectorBuffer> VectorBuffer::Slice(const LogicalType &type, idx_t offset, idx_t end) {
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		// constant vectors do not need to get sliced
		return nullptr;
	}
	return SliceInternal(type, offset, end);
}

buffer_ptr<VectorBuffer> VectorBuffer::Slice(const LogicalType &type, const SelectionVector &sel, idx_t count) {
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		// constant vectors do not need to get sliced
		return nullptr;
	}
	return SliceInternal(type, sel, count);
}

buffer_ptr<VectorBuffer> VectorBuffer::SliceWithCache(SelCache &cache, const LogicalType &type,
                                                      const SelectionVector &sel, idx_t count) {
	return Slice(type, sel, count);
}

buffer_ptr<VectorBuffer> VectorBuffer::SliceInternal(const LogicalType &type, idx_t offset, idx_t end) {
	// we can slice the data directly only for standard vectors
	// for non-flat vectors slice using a selection vector instead
	idx_t count = end - offset;
	SelectionVector sel(count);
	for (idx_t i = 0; i < count; i++) {
		sel.set_index(i, offset + i);
	}
	return Slice(type, sel, count);
}

buffer_ptr<VectorBuffer> VectorBuffer::SliceInternal(const LogicalType &type, const SelectionVector &sel, idx_t count) {
	// default slice: flatten and then wrap in a dictionary
	auto new_buffer = Flatten(type, sel, count);

	Vector child_vector(type, new_buffer);
	auto entry = make_shared_ptr<DictionaryEntry>(std::move(child_vector));
	return make_buffer<DictionaryBuffer>(sel, std::move(entry));
}

void VectorBuffer::SetValue(const LogicalType &type, idx_t index, const Value &val) {
	throw InternalException("SetValue not supported for this buffer type");
}

Value VectorBuffer::GetValue(const LogicalType &type, idx_t index) const {
	throw InternalException("Unimplemented GetValue for this buffer type");
}

PinnedBufferHolder::PinnedBufferHolder(BufferHandle handle) : handle(std::move(handle)) {
}

PinnedBufferHolder::~PinnedBufferHolder() {
}

} // namespace duckdb
