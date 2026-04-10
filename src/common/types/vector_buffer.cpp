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

void VectorBuffer::FindResizeInfos(Vector &vector, duckdb::vector<ResizeInfo> &resize_infos, idx_t multiplier) {
	auto type_size = GetTypeIdSize(vector.GetType().InternalType());
	optional_ptr<VectorBuffer> buf_ptr = type_size ? this : nullptr;
	resize_infos.emplace_back(vector, buf_ptr, multiplier);
}

void VectorBuffer::Resize(Vector &vector, idx_t current_size, idx_t new_size) {
	// Obtain the resize information for each (nested) vector.
	duckdb::vector<ResizeInfo> resize_infos;
	FindResizeInfos(vector, resize_infos, 1);

	for (auto &resize_info_entry : resize_infos) {
		// Resize the validity mask.
		auto new_validity_size = new_size * resize_info_entry.multiplier;
		resize_info_entry.vec.buffer->GetValidityMask().Resize(new_validity_size);

		// For nested data types, we only need to resize the validity mask.
		if (!resize_info_entry.data) {
			continue;
		}

		auto type_size = GetTypeIdSize(resize_info_entry.vec.GetType().InternalType());
		auto old_data_size = current_size * type_size * resize_info_entry.multiplier * sizeof(data_t);
		auto target_size = new_size * type_size * resize_info_entry.multiplier * sizeof(data_t);

		if (target_size > DConstants::MAX_VECTOR_SIZE) {
			throw OutOfRangeException("Cannot resize vector to %s: maximum allowed vector size is %s",
			                          StringUtil::BytesToHumanReadableString(target_size),
			                          StringUtil::BytesToHumanReadableString(DConstants::MAX_VECTOR_SIZE));
		}
		auto stored_allocator = resize_info_entry.buffer->GetAllocator();
		auto &allocator = stored_allocator ? *stored_allocator : Allocator::DefaultAllocator();
		auto new_data = allocator.Allocate(target_size);
		memcpy(new_data.get(), resize_info_entry.data, old_data_size);
		auto resized_validity = std::move(resize_info_entry.vec.buffer->GetValidityMask());
		buffer_ptr<VectorBuffer> new_buffer;
		if (resize_info_entry.vec.GetType().InternalType() == PhysicalType::LIST) {
			auto &old_buffer = resize_info_entry.vec.buffer->Cast<VectorListBuffer>();
			new_buffer = make_buffer<VectorListBuffer>(std::move(new_data), old_buffer);
		} else if (resize_info_entry.vec.GetType().InternalType() == PhysicalType::VARCHAR) {
			auto &old_buffer = resize_info_entry.vec.buffer->Cast<VectorStringBuffer>();
			new_buffer = make_buffer<VectorStringBuffer>(std::move(new_data), old_buffer);
		} else {
			new_buffer = make_buffer<StandardVectorBuffer>(std::move(new_data));
		}
		new_buffer->GetValidityMask() = std::move(resized_validity);
		resize_info_entry.buffer = new_buffer.get();
		resize_info_entry.vec.buffer = std::move(new_buffer);
	}
}

void VectorBuffer::ToUnifiedFormat(idx_t count, UnifiedVectorFormat &format) const {
	throw InternalException("ToUnifiedFormat not supported for this buffer type - flatten first");
}

buffer_ptr<VectorBuffer> VectorBuffer::Slice(const LogicalType &type, const VectorBuffer &source, idx_t offset, idx_t end) {
	throw InternalException("Unimplemented Slice with offset for this buffer type");
}

buffer_ptr<VectorBuffer> VectorBuffer::Slice(const SelectionVector &sel, idx_t count) {
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		return nullptr;
	}
	// default: return nullptr to indicate the caller should wrap in a dictionary
	return nullptr;
}

void VectorBuffer::SetValue(const LogicalType &type, idx_t index, const Value &val) {
	throw InternalException("SetValue not supported for this buffer type");
}

Value VectorBuffer::GetValue(const LogicalType &type, idx_t index) const {
	throw InternalException("Unimplemented GetValue for this buffer type");
}

buffer_ptr<VectorBuffer> VectorBuffer::Flatten(const LogicalType &type, const SelectionVector &sel, idx_t count) {
	throw InternalException("Unimplemented type for flatten");
}

PinnedBufferHolder::PinnedBufferHolder(BufferHandle handle) : handle(std::move(handle)) {
}

PinnedBufferHolder::~PinnedBufferHolder() {
}

} // namespace duckdb
