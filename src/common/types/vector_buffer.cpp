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

buffer_ptr<VectorBuffer> VectorBuffer::CreateStandardVector(PhysicalType type, capacity_t capacity) {
	if (type == PhysicalType::LIST) {
		throw InternalException("VectorBuffer::CreateStandardVector requires full list type");
	}
	if (type == PhysicalType::VARCHAR) {
		return make_buffer<VectorStringBuffer>(capacity);
	}
	return make_buffer<StandardVectorBuffer>(capacity, GetTypeIdSize(type));
}

buffer_ptr<VectorBuffer> VectorBuffer::CreateStandardVector(const LogicalType &type, capacity_t capacity) {
	if (type.InternalType() == PhysicalType::LIST) {
		throw InternalException("VectorBuffer::CreateStandardVector not supported for list");
	}
	return VectorBuffer::CreateStandardVector(type.InternalType(), capacity);
}

void VectorBuffer::SetVectorSize(idx_t new_size) {
	if (HasSize() && Size() == new_size) {
		// nop: size is the same as previous size
		return;
	}
	switch (vector_type) {
	case VectorType::CONSTANT_VECTOR:
		break;
	case VectorType::FLAT_VECTOR:
		if (new_size > Capacity()) {
			throw InternalException(
			    "Vector::SetSize out of range - trying to set size to %d for vector with capacity %d", new_size,
			    Capacity());
		}
		break;
	default:
		throw InternalException("VectorBuffer::SetVectorSize can only be used for flat or constant vectors, other "
		                        "vector types have their size set on creation");
	}
	v_size = new_size;
}

idx_t VectorBuffer::GetDataSize(const LogicalType &type, idx_t count) const {
	idx_t size = 0;
	// uncompressed size of individual data entries
	size += GetTypeIdSize(type.InternalType()) * count;
	// size of validity mask
	size += GetValidityMask().GetAllocationSize();
	// size stored in aux buffers
	if (auxiliary_data) {
		for (auto &aux_data : auxiliary_data->data) {
			size += aux_data->GetAllocationSize();
		}
	}
	return size;
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

void VectorBuffer::Resize(idx_t current_size, idx_t new_size) {
	throw InternalException("VectorBuffer::Resize not supported for this vector type");
}

void VectorBuffer::ToUnifiedFormat(idx_t count, UnifiedVectorFormat &format) const {
	throw InternalException("ToUnifiedFormat not supported for this buffer type - flatten first");
}

buffer_ptr<VectorBuffer> VectorBuffer::Flatten(const LogicalType &type, idx_t count) const {
	// FIXME: this should just be using size.GetIndex()...
	if (v_size.IsValid()) {
		if (count > v_size.GetIndex()) {
			throw InternalException("Flatten called with count that exceeds the size of the vector");
		}
		count = v_size.GetIndex();
	}
	auto result = FlattenSliceInternal(type, *FlatVector::IncrementalSelectionVector(), count);
	if (result && (!result->HasSize() || result->Size() != count)) {
		throw InternalException("FlattenSliceInternal did not set size correctly");
	}
	return result;
}

buffer_ptr<VectorBuffer> VectorBuffer::FlattenSlice(const LogicalType &type, const SelectionVector &sel,
                                                    idx_t count) const {
	buffer_ptr<VectorBuffer> result;
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		// if the vector is a constant vector the input selection vector does not matter
		// we always need to select the first value
		SelectionVector owned_sel;
		auto &constant_sel = *ConstantVector::ZeroSelectionVector(count, owned_sel);
		result = FlattenSliceInternal(type, constant_sel, count);
	} else {
		result = FlattenSliceInternal(type, sel, count);
	}
	if (result && (!result->HasSize() || result->Size() != count)) {
		throw InternalException("FlattenSliceInternal did not set size correctly");
	}
	return result;
}

buffer_ptr<VectorBuffer> VectorBuffer::FlattenSliceInternal(const LogicalType &type, const SelectionVector &sel,
                                                            idx_t count) const {
	throw InternalException("Unimplemented type for flatten");
}

buffer_ptr<VectorBuffer> VectorBuffer::Slice(const LogicalType &type, idx_t offset, idx_t end) {
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		// constant vectors do not need to get sliced - but we do need to update the count
		return ConstantSlice(type, count_t(end - offset));
	}
	auto result = SliceInternal(type, offset, end);
	if (result) {
		if (!result->HasSize() || result->Size() != end - offset) {
			throw InternalException("Slice with offset,end did not set size correctly");
		}
	}
	return result;
}

buffer_ptr<VectorBuffer> VectorBuffer::Slice(const LogicalType &type, const SelectionVector &sel, idx_t count) {
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		// constant vectors do not need to get sliced - but we do need to update the count
		return ConstantSlice(type, count_t(count));
	}
	auto result = SliceInternal(type, sel, count);
	if (result && v_size.IsValid()) {
		if (!result->HasSize() || result->Size() != count) {
			throw InternalException("Slice with count did not set size correctly");
		}
	}
	return result;
}

buffer_ptr<VectorBuffer> VectorBuffer::ConstantSlice(const LogicalType &type, count_t count) {
	if (HasSize() && count == Size()) {
		// if the size is already set correctly we don't need to do do anything
		return nullptr;
	}
	return ConstantSliceInternal(type, count);
}

buffer_ptr<VectorBuffer> VectorBuffer::ConstantSliceInternal(const LogicalType &type, count_t count) {
	throw InternalException("Constant slice not implemented for this vector buffer");
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
	// default slice: flatten with a selection vector
	return FlattenSlice(type, sel, count);
}

idx_t VectorBuffer::GetReserveSize(idx_t required_capacity) {
	if (required_capacity > DConstants::MAX_VECTOR_SIZE) {
		// overflow: throw an exception
		throw OutOfRangeException("Cannot resize vector to %d rows: maximum allowed vector size is %s",
		                          required_capacity,
		                          StringUtil::BytesToHumanReadableString(DConstants::MAX_VECTOR_SIZE));
	}
	return NextPowerOfTwo(required_capacity);
}

void VectorBuffer::Reserve(idx_t required_capacity, VectorAppendMode append_mode) {
	auto capacity = Capacity();
	if (required_capacity <= capacity) {
		// enough space
		return;
	}
	if (append_mode == VectorAppendMode::ERROR_ON_NO_SPACE) {
		throw InternalException("Can't append to vector without resizing - but resizing was explicitly disabled");
	}
	auto new_capacity = GetReserveSize(required_capacity);
	D_ASSERT(new_capacity >= required_capacity);
	Resize(capacity, new_capacity);
}

void VectorBuffer::AppendValue(const LogicalType &type, const Value &val, VectorAppendMode append_mode) {
	if (!HasSize()) {
		throw InternalException("Can only append to vector with a size");
	}
	auto new_capacity = v_size.GetIndex() + 1;
	Reserve(new_capacity, append_mode);
	SetValue(type, v_size.GetIndex(), val);
	v_size = v_size.GetIndex() + 1;
}

void VectorBuffer::Append(const Vector &source, const SelectionVector &sel, idx_t append_size,
                          VectorAppendMode append_mode) {
	if (!HasSize()) {
		throw InternalException("Cannot append to vector without size");
	}
	auto current_size = Size();
	Reserve(current_size + append_size, append_mode);
	Copy(source, sel, append_size, 0, current_size, append_size);
	v_size = v_size.GetIndex() + append_size;
}

void VectorBuffer::SetValue(const LogicalType &type, idx_t index, const Value &val) {
	throw InternalException("SetValue not supported for this buffer type");
}

void VectorBuffer::Copy(const Vector &source_p, const SelectionVector &source_sel, idx_t source_count,
                        idx_t source_offset, idx_t target_offset, idx_t copy_count) {
	if (copy_count == 0) {
		return;
	}
	// traverse vector types until we have a flat / constant vector as source
	SelectionVector owned_sel;
	const_reference<SelectionVector> sel_ref(source_sel);
	const_reference<Vector> source_ref(source_p);
	bool finished = false;
	while (!finished) {
		auto &source = source_ref.get();
		auto &sel = sel_ref.get();
		switch (source.GetVectorType()) {
		case VectorType::DICTIONARY_VECTOR: {
			// dictionary vector: merge selection vectors
			auto &child = DictionaryVector::Child(source);
			auto &dict_sel = DictionaryVector::SelVector(source);
			// merge the selection vectors and verify the child
			if (sel.IsSet()) {
				auto new_buffer = dict_sel.Slice(sel, source_count);
				owned_sel.Initialize(new_buffer);
				sel_ref = owned_sel;
			} else {
				sel_ref = dict_sel;
			}
			source_ref = child;
			break;
		}
		case VectorType::CONSTANT_VECTOR:
			sel_ref = *ConstantVector::ZeroSelectionVector(copy_count, owned_sel);
			finished = true;
			break;
		case VectorType::FLAT_VECTOR:
			finished = true;
			break;
		default: {
			// for exotic types we flatten followed by copying
			Vector flattened_vector(Vector::Ref(source));
			flattened_vector.Flatten(sel, source_offset + source_count);
			Copy(flattened_vector, *FlatVector::IncrementalSelectionVector(), source_count, source_offset,
			     target_offset, copy_count);
			return;
		}
		}
	}

	// copy over the nullmask
	auto &source = source_ref.get();
	auto &sel = sel_ref.get();
	auto source_type = source_ref.get().GetVectorType();
	D_ASSERT(source_type == VectorType::CONSTANT_VECTOR || source_type == VectorType::FLAT_VECTOR);
	auto &validity = GetValidityMask();
	if (source_type == VectorType::CONSTANT_VECTOR) {
		const bool valid = !ConstantVector::IsNull(source);
		for (idx_t i = 0; i < copy_count; i++) {
			validity.Set(target_offset + i, valid);
		}
	} else {
		auto &smask = FlatVector::Validity(source);
		validity.CopySel(smask, sel, source_offset, target_offset, copy_count);
	}

	// now call CopyInternal to perform the copy of the data
	CopyInternal(source, sel, source_count, source_offset, target_offset, copy_count);
}

void VectorBuffer::CopyInternal(const Vector &source, const SelectionVector &source_sel, idx_t source_count,
                                idx_t source_offset, idx_t target_offset, idx_t copy_count) {
	throw InternalException("Copying data into this buffer type is not supported");
}

Value VectorBuffer::GetValue(const LogicalType &type, idx_t index) const {
	throw InternalException("Unimplemented GetValue for this buffer type");
}

PinnedBufferHolder::PinnedBufferHolder(BufferHandle handle) : handle(std::move(handle)) {
}

PinnedBufferHolder::~PinnedBufferHolder() {
}

} // namespace duckdb
