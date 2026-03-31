#include "duckdb/common/vector/array_vector.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
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
		return make_buffer<VectorStringBuffer>(capacity * GetTypeIdSize(type));
	}
	return make_buffer<StandardVectorBuffer>(capacity * GetTypeIdSize(type));
}

buffer_ptr<VectorBuffer> VectorBuffer::CreateConstantVector(PhysicalType type) {
	if (type == PhysicalType::LIST) {
		throw InternalException("VectorBuffer::CreateConstantVector requires full list type");
	}
	if (type == PhysicalType::VARCHAR) {
		return make_buffer<VectorStringBuffer>(GetTypeIdSize(type));
	}
	return make_buffer<StandardVectorBuffer>(GetTypeIdSize(type));
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

VectorFSSTStringBuffer::VectorFSSTStringBuffer() : VectorStringBuffer(VectorBufferType::FSST_BUFFER) {
}

VectorStructBuffer::VectorStructBuffer() : VectorBuffer(VectorBufferType::STRUCT_BUFFER) {
}

VectorStructBuffer::VectorStructBuffer(const LogicalType &type, idx_t capacity)
    : VectorBuffer(VectorBufferType::STRUCT_BUFFER) {
	auto &child_types = StructType::GetChildTypes(type);
	for (auto &child_type : child_types) {
		children.emplace_back(child_type.second, capacity);
	}
}

VectorStructBuffer::VectorStructBuffer(Vector &other, const SelectionVector &sel, idx_t count)
    : VectorBuffer(VectorBufferType::STRUCT_BUFFER) {
	auto &other_vector = StructVector::GetEntries(other);
	for (auto &child_vector : other_vector) {
		children.emplace_back(child_vector, sel, count);
	}
}

VectorStructBuffer::~VectorStructBuffer() {
}

VectorListBuffer::VectorListBuffer(Allocator &allocator, idx_t capacity, unique_ptr<Vector> vector,
                                   idx_t child_capacity)
    : VectorBuffer(VectorBufferType::LIST_BUFFER), child(std::move(vector)), capacity(child_capacity) {
	if (capacity > 0) {
		allocated_data = allocator.Allocate(capacity * sizeof(list_entry_t));
		data_ptr = allocated_data.get();
	}
}
VectorListBuffer::VectorListBuffer(Allocator &allocator, idx_t capacity, const LogicalType &list_type,
                                   idx_t child_capacity)
    : VectorBuffer(VectorBufferType::LIST_BUFFER),
      child(make_uniq<Vector>(ListType::GetChildType(list_type), child_capacity)), capacity(child_capacity) {
	if (capacity > 0) {
		allocated_data = allocator.Allocate(capacity * sizeof(list_entry_t));
		data_ptr = allocated_data.get();
	}
}

VectorListBuffer::VectorListBuffer(idx_t capacity, const LogicalType &list_type, idx_t child_capacity)
    : VectorListBuffer(Allocator::DefaultAllocator(), capacity, list_type, child_capacity) {
}

VectorListBuffer::VectorListBuffer(data_ptr_t data, const VectorListBuffer &parent)
    : VectorBuffer(VectorBufferType::LIST_BUFFER), data_ptr(data), capacity(parent.capacity), size(parent.size) {
	child = make_uniq<Vector>(Vector::Ref(parent.GetChild()));
}

VectorListBuffer::VectorListBuffer(AllocatedData allocated_data_p, const VectorListBuffer &parent)
    : VectorBuffer(VectorBufferType::LIST_BUFFER), allocated_data(std::move(allocated_data_p)),
      capacity(parent.capacity), size(parent.size) {
	child = make_uniq<Vector>(Vector::Ref(parent.GetChild()));
	data_ptr = allocated_data.get();
}

void VectorListBuffer::Reserve(idx_t to_reserve) {
	if (to_reserve > capacity) {
		if (to_reserve > DConstants::MAX_VECTOR_SIZE) {
			// overflow: throw an exception
			throw OutOfRangeException("Cannot resize vector to %d rows: maximum allowed vector size is %s", to_reserve,
			                          StringUtil::BytesToHumanReadableString(DConstants::MAX_VECTOR_SIZE));
		}
		idx_t new_capacity = NextPowerOfTwo(to_reserve);
		D_ASSERT(new_capacity >= to_reserve);
		child->Resize(capacity, new_capacity);
		capacity = new_capacity;
	}
}

void VectorListBuffer::Append(const Vector &to_append, idx_t to_append_size, idx_t source_offset) {
	Reserve(size + to_append_size - source_offset);
	VectorOperations::Copy(to_append, *child, to_append_size, source_offset, size);
	size += to_append_size - source_offset;
}

void VectorListBuffer::Append(const Vector &to_append, const SelectionVector &sel, idx_t to_append_size,
                              idx_t source_offset) {
	Reserve(size + to_append_size - source_offset);
	VectorOperations::Copy(to_append, *child, sel, to_append_size, source_offset, size);
	size += to_append_size - source_offset;
}

void VectorListBuffer::PushBack(const Value &insert) {
	while (size + 1 > capacity) {
		child->Resize(capacity, capacity * 2);
		capacity *= 2;
	}
	child->SetValue(size++, insert);
}

void VectorListBuffer::SetCapacity(idx_t new_capacity) {
	this->capacity = new_capacity;
}

void VectorListBuffer::SetSize(idx_t new_size) {
	this->size = new_size;
}

VectorListBuffer::~VectorListBuffer() {
}

VectorArrayBuffer::VectorArrayBuffer(unique_ptr<Vector> child_vector, idx_t array_size, idx_t initial_capacity)
    : VectorBuffer(VectorBufferType::ARRAY_BUFFER), child(std::move(child_vector)), array_size(array_size),
      size(initial_capacity) {
	D_ASSERT(array_size != 0);
}

VectorArrayBuffer::VectorArrayBuffer(const LogicalType &array, idx_t initial)
    : VectorBuffer(VectorBufferType::ARRAY_BUFFER),
      child(make_uniq<Vector>(ArrayType::GetChildType(array), initial * ArrayType::GetSize(array))),
      array_size(ArrayType::GetSize(array)), size(initial) {
	// initialize the child array with (array_size * size) ^
	D_ASSERT(!ArrayType::IsAnySize(array));
}

VectorArrayBuffer::~VectorArrayBuffer() {
}

Vector &VectorArrayBuffer::GetChild() {
	return *child;
}

idx_t VectorArrayBuffer::GetArraySize() {
	return array_size;
}

idx_t VectorArrayBuffer::GetChildSize() {
	return size * array_size;
}

PinnedBufferHolder::PinnedBufferHolder(BufferHandle handle) : handle(std::move(handle)) {
}

PinnedBufferHolder::~PinnedBufferHolder() {
}

ShreddedVectorBuffer::ShreddedVectorBuffer(Vector &shredded_data_p)
    : VectorBuffer(VectorBufferType::SHREDDED_BUFFER), shredded_data(make_uniq<Vector>(Vector::Ref(shredded_data_p))) {
}

ShreddedVectorBuffer::~ShreddedVectorBuffer() {
}

} // namespace duckdb
