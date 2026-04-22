#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/array_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/types/bignum.hpp"

namespace duckdb {
StandardVectorBuffer::StandardVectorBuffer(Allocator &allocator, idx_t capacity_p, idx_t type_size_p)
    : VectorBuffer(VectorType::FLAT_VECTOR, VectorBufferType::STANDARD_BUFFER), data_ptr(nullptr),
      type_size(type_size_p), capacity(capacity_p) {
	if (capacity > 0) {
		if (type_size == 0) {
			throw InternalException("eek");
		}
		allocated_data = allocator.Allocate(capacity * type_size);
		data_ptr = allocated_data.get();
		// resize the validity
		validity.Resize(capacity);
	}
}
StandardVectorBuffer::StandardVectorBuffer(idx_t capacity, idx_t type_size_p)
    : StandardVectorBuffer(Allocator::DefaultAllocator(), capacity, type_size_p) {
}
StandardVectorBuffer::StandardVectorBuffer(data_ptr_t data_ptr_p, idx_t capacity_p, idx_t type_size_p)
    : VectorBuffer(VectorType::FLAT_VECTOR, VectorBufferType::STANDARD_BUFFER), data_ptr(data_ptr_p),
      type_size(type_size_p), capacity(capacity_p) {
}
StandardVectorBuffer::StandardVectorBuffer(AllocatedData &&data_p, idx_t capacity_p, idx_t type_size_p)
    : VectorBuffer(VectorType::FLAT_VECTOR, VectorBufferType::STANDARD_BUFFER), data_ptr(data_p.get()),
      type_size(type_size_p), capacity(capacity_p), allocated_data(std::move(data_p)) {
}

void StandardVectorBuffer::SetVectorType(VectorType new_vector_type) {
	vector_type = new_vector_type;
}

void StandardVectorBuffer::ResetCapacity(idx_t capacity) {
	this->capacity = capacity;
	validity.Reset(capacity);
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

buffer_ptr<VectorBuffer> StandardVectorBuffer::SliceInternal(const LogicalType &type, idx_t offset, idx_t end) {
	D_ASSERT(end <= capacity);
	auto offset_ptr = data_ptr + type_size * offset;
	auto result = make_buffer<StandardVectorBuffer>(offset_ptr, end - offset, type_size);
	result->GetValidityMask().Slice(validity, offset, end - offset);
	return result;
}

buffer_ptr<VectorBuffer> StandardVectorBuffer::SliceInternal(const LogicalType &type, const SelectionVector &sel,
                                                             idx_t count) {
	Vector child_vector(type, shared_from_this());
	auto entry = make_shared_ptr<DictionaryEntry>(std::move(child_vector));
	return make_buffer<DictionaryBuffer>(sel, count, std::move(entry));
}

buffer_ptr<VectorBuffer> StandardVectorBuffer::CreateBuffer(AllocatedData &&new_data, idx_t new_capacity) const {
	return make_buffer<StandardVectorBuffer>(std::move(new_data), new_capacity, type_size);
}

void StandardVectorBuffer::Resize(idx_t current_size, idx_t new_size) {
	D_ASSERT(current_size <= capacity);
	auto old_byte_count = current_size * type_size;
	auto target_byte_count = new_size * type_size;

	// We have an upper limit of 128GB for a single vector.
	if (target_byte_count > DConstants::MAX_VECTOR_SIZE) {
		throw OutOfRangeException("Cannot resize vector to %s: maximum allowed vector size is %s",
		                          StringUtil::BytesToHumanReadableString(target_byte_count),
		                          StringUtil::BytesToHumanReadableString(DConstants::MAX_VECTOR_SIZE));
	}
	// Copy the data buffer to a resized buffer.
	auto stored_allocator = GetAllocator();
	auto &allocator = stored_allocator ? *stored_allocator : Allocator::DefaultAllocator();
	auto new_data = allocator.Allocate(target_byte_count);
	memcpy(new_data.get(), data_ptr, old_byte_count);

	// update the buffer structure in-place
	allocated_data = std::move(new_data);
	data_ptr = allocated_data.get();
	capacity = new_size;
	// resize the validity mask
	validity.Resize(new_size);
}

template <idx_t TYPE_SIZE>
void FixedFlattenCopy(data_ptr_t target, const_data_ptr_t source, const SelectionVector &sel, idx_t count) {
	for (idx_t i = 0; i < count; i++) {
		auto src_idx = sel.get_index(i);
		memcpy(target + i * TYPE_SIZE, source + src_idx * TYPE_SIZE, TYPE_SIZE);
	}
}

void FlattenVectorBuffer(data_ptr_t target, const_data_ptr_t source, const SelectionVector &sel, idx_t count,
                         idx_t type_size) {
	switch (type_size) {
	case 1:
		FixedFlattenCopy<1>(target, source, sel, count);
		break;
	case 2:
		FixedFlattenCopy<2>(target, source, sel, count);
		break;
	case 4:
		FixedFlattenCopy<4>(target, source, sel, count);
		break;
	case 8:
		FixedFlattenCopy<8>(target, source, sel, count);
		break;
	case 16:
		FixedFlattenCopy<16>(target, source, sel, count);
		break;
	default:
		// fallback: use non-fixed-width copy
		for (idx_t i = 0; i < count; i++) {
			auto src_idx = sel.get_index(i);
			memcpy(target + i * type_size, source + src_idx * type_size, type_size);
		}
		break;
	}
}

buffer_ptr<VectorBuffer> StandardVectorBuffer::Flatten(const LogicalType &type, idx_t count) const {
	if (vector_type == VectorType::FLAT_VECTOR) {
		// already a flat vector - bail
		return nullptr;
	}
	return FlattenSlice(type, *FlatVector::IncrementalSelectionVector(), count);
}

buffer_ptr<VectorBuffer> StandardVectorBuffer::FlattenSliceInternal(const LogicalType &type, const SelectionVector &sel,
												   idx_t count) const {
	// allocate the new buffer
	auto allocated_count = MaxValue<idx_t>(STANDARD_VECTOR_SIZE, count);
	auto target_byte_count = allocated_count * type_size;
	auto stored_allocator = GetAllocator();
	auto &allocator = stored_allocator ? *stored_allocator : Allocator::DefaultAllocator();
	auto new_data = allocator.Allocate(target_byte_count);
	// copy data using sel
	FlattenVectorBuffer(new_data.get(), data_ptr, sel, count, type_size);

	auto result = CreateBuffer(std::move(new_data), count);
	result->SetVectorSize(count);
	// copy validity using sel
	auto &result_validity = result->GetValidityMask();
	result_validity.Resize(allocated_count);
	result_validity.CopySel(validity, sel, 0, 0, count);
	return result;
}

void StandardVectorBuffer::ToUnifiedFormat(idx_t count, UnifiedVectorFormat &format) const {
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		format.sel = ConstantVector::ZeroSelectionVector(count, format.owned_sel);
	} else {
		format.sel = FlatVector::IncrementalSelectionVector();
	}
	format.data = data_ptr;
	format.validity = validity;
}

template <idx_t TYPE_SIZE>
void FixedSizeCopy(data_ptr_t target_data, const_data_ptr_t source_data, const SelectionVector &sel,
                   idx_t base_source_offset, idx_t base_target_offset, idx_t copy_count) {
	for (idx_t i = 0; i < copy_count; i++) {
		auto source_idx = sel.get_index(base_source_offset + i);
		auto target_offset = (base_target_offset + i) * TYPE_SIZE;
		auto source_offset = source_idx * TYPE_SIZE;
		memcpy(target_data + target_offset, source_data + source_offset, TYPE_SIZE);
	}
}

void CopyVectorBuffer(data_ptr_t target_data, const_data_ptr_t source_data, const SelectionVector &sel,
                      idx_t base_source_offset, idx_t base_target_offset, idx_t copy_count, idx_t type_size) {
	switch (type_size) {
	case 1:
		FixedSizeCopy<1>(target_data, source_data, sel, base_source_offset, base_target_offset, copy_count);
		break;
	case 2:
		FixedSizeCopy<2>(target_data, source_data, sel, base_source_offset, base_target_offset, copy_count);
		break;
	case 4:
		FixedSizeCopy<4>(target_data, source_data, sel, base_source_offset, base_target_offset, copy_count);
		break;
	case 8:
		FixedSizeCopy<8>(target_data, source_data, sel, base_source_offset, base_target_offset, copy_count);
		break;
	case 16:
		FixedSizeCopy<16>(target_data, source_data, sel, base_source_offset, base_target_offset, copy_count);
		break;
	default:
		// fallback: use non-fixed-width copy
		for (idx_t i = 0; i < copy_count; i++) {
			auto source_idx = sel.get_index(base_source_offset + i);
			auto target_offset = (base_target_offset + i) * type_size;
			auto source_offset = source_idx * type_size;
			memcpy(target_data + target_offset, source_data + source_offset, type_size);
		}
		break;
	}
}

void StandardVectorBuffer::CopyInternal(const Vector &source, const SelectionVector &source_sel, idx_t source_count,
                                        idx_t source_offset, idx_t target_offset, idx_t copy_count) {
	// now copy over the data
	const_data_ptr_t source_data;
	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		source_data = ConstantVector::GetData(source);
	} else {
		source_data = FlatVector::GetData(source);
	}

	CopyVectorBuffer(data_ptr, source_data, source_sel, source_offset, target_offset, copy_count, type_size);
}

void StandardVectorBuffer::SetValue(const LogicalType &type, idx_t index, const Value &val) {
	if (!val.IsNull() && val.type() != type) {
		SetValue(type, index, val.DefaultCastAs(type));
		return;
	}
	if (index >= capacity) {
		throw InvalidInputException("Vector::SetValue index %d is out of range for vector with capacity %d", index,
		                            capacity);
	}
	validity.Set(index, !val.IsNull());
	if (val.IsNull()) {
		return;
	}
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		reinterpret_cast<bool *>(data_ptr)[index] = val.GetValueUnsafe<bool>();
		break;
	case PhysicalType::INT8:
		reinterpret_cast<int8_t *>(data_ptr)[index] = val.GetValueUnsafe<int8_t>();
		break;
	case PhysicalType::INT16:
		reinterpret_cast<int16_t *>(data_ptr)[index] = val.GetValueUnsafe<int16_t>();
		break;
	case PhysicalType::INT32:
		reinterpret_cast<int32_t *>(data_ptr)[index] = val.GetValueUnsafe<int32_t>();
		break;
	case PhysicalType::INT64:
		reinterpret_cast<int64_t *>(data_ptr)[index] = val.GetValueUnsafe<int64_t>();
		break;
	case PhysicalType::INT128:
		reinterpret_cast<hugeint_t *>(data_ptr)[index] = val.GetValueUnsafe<hugeint_t>();
		break;
	case PhysicalType::UINT8:
		reinterpret_cast<uint8_t *>(data_ptr)[index] = val.GetValueUnsafe<uint8_t>();
		break;
	case PhysicalType::UINT16:
		reinterpret_cast<uint16_t *>(data_ptr)[index] = val.GetValueUnsafe<uint16_t>();
		break;
	case PhysicalType::UINT32:
		reinterpret_cast<uint32_t *>(data_ptr)[index] = val.GetValueUnsafe<uint32_t>();
		break;
	case PhysicalType::UINT64:
		reinterpret_cast<uint64_t *>(data_ptr)[index] = val.GetValueUnsafe<uint64_t>();
		break;
	case PhysicalType::UINT128:
		reinterpret_cast<uhugeint_t *>(data_ptr)[index] = val.GetValueUnsafe<uhugeint_t>();
		break;
	case PhysicalType::FLOAT:
		reinterpret_cast<float *>(data_ptr)[index] = val.GetValueUnsafe<float>();
		break;
	case PhysicalType::DOUBLE:
		reinterpret_cast<double *>(data_ptr)[index] = val.GetValueUnsafe<double>();
		break;
	case PhysicalType::INTERVAL:
		reinterpret_cast<interval_t *>(data_ptr)[index] = val.GetValueUnsafe<interval_t>();
		break;
	default:
		throw InternalException("Unimplemented type for StandardVectorBuffer::SetValue");
	}
}

Value StandardVectorBuffer::GetValue(const LogicalType &type, idx_t index) const {
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		index = 0;
	}
	if (!validity.RowIsValid(index)) {
		return Value(type);
	}
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return Value::BOOLEAN(reinterpret_cast<const bool *>(data_ptr)[index]);
	case LogicalTypeId::TINYINT:
		return Value::TINYINT(reinterpret_cast<const int8_t *>(data_ptr)[index]);
	case LogicalTypeId::SMALLINT:
		return Value::SMALLINT(reinterpret_cast<const int16_t *>(data_ptr)[index]);
	case LogicalTypeId::INTEGER:
		return Value::INTEGER(reinterpret_cast<const int32_t *>(data_ptr)[index]);
	case LogicalTypeId::DATE:
		return Value::DATE(reinterpret_cast<const date_t *>(data_ptr)[index]);
	case LogicalTypeId::TIME:
		return Value::TIME(reinterpret_cast<const dtime_t *>(data_ptr)[index]);
	case LogicalTypeId::TIME_NS:
		return Value::TIME_NS(reinterpret_cast<const dtime_ns_t *>(data_ptr)[index]);
	case LogicalTypeId::TIME_TZ:
		return Value::TIMETZ(reinterpret_cast<const dtime_tz_t *>(data_ptr)[index]);
	case LogicalTypeId::BIGINT:
		return Value::BIGINT(reinterpret_cast<const int64_t *>(data_ptr)[index]);
	case LogicalTypeId::UTINYINT:
		return Value::UTINYINT(reinterpret_cast<const uint8_t *>(data_ptr)[index]);
	case LogicalTypeId::USMALLINT:
		return Value::USMALLINT(reinterpret_cast<const uint16_t *>(data_ptr)[index]);
	case LogicalTypeId::UINTEGER:
		return Value::UINTEGER(reinterpret_cast<const uint32_t *>(data_ptr)[index]);
	case LogicalTypeId::UBIGINT:
		return Value::UBIGINT(reinterpret_cast<const uint64_t *>(data_ptr)[index]);
	case LogicalTypeId::TIMESTAMP:
		return Value::TIMESTAMP(reinterpret_cast<const timestamp_t *>(data_ptr)[index]);
	case LogicalTypeId::TIMESTAMP_NS:
		return Value::TIMESTAMPNS(reinterpret_cast<const timestamp_ns_t *>(data_ptr)[index]);
	case LogicalTypeId::TIMESTAMP_MS:
		return Value::TIMESTAMPMS(reinterpret_cast<const timestamp_ms_t *>(data_ptr)[index]);
	case LogicalTypeId::TIMESTAMP_SEC:
		return Value::TIMESTAMPSEC(reinterpret_cast<const timestamp_sec_t *>(data_ptr)[index]);
	case LogicalTypeId::TIMESTAMP_TZ:
		return Value::TIMESTAMPTZ(reinterpret_cast<const timestamp_tz_t *>(data_ptr)[index]);
	case LogicalTypeId::HUGEINT:
		return Value::HUGEINT(reinterpret_cast<const hugeint_t *>(data_ptr)[index]);
	case LogicalTypeId::UHUGEINT:
		return Value::UHUGEINT(reinterpret_cast<const uhugeint_t *>(data_ptr)[index]);
	case LogicalTypeId::UUID:
		return Value::UUID(reinterpret_cast<const hugeint_t *>(data_ptr)[index]);
	case LogicalTypeId::DECIMAL: {
		auto width = DecimalType::GetWidth(type);
		auto scale = DecimalType::GetScale(type);
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return Value::DECIMAL(reinterpret_cast<const int16_t *>(data_ptr)[index], width, scale);
		case PhysicalType::INT32:
			return Value::DECIMAL(reinterpret_cast<const int32_t *>(data_ptr)[index], width, scale);
		case PhysicalType::INT64:
			return Value::DECIMAL(reinterpret_cast<const int64_t *>(data_ptr)[index], width, scale);
		case PhysicalType::INT128:
			return Value::DECIMAL(reinterpret_cast<const hugeint_t *>(data_ptr)[index], width, scale);
		default:
			throw InternalException("Physical type '%s' has a width bigger than 38, which is not supported",
			                        TypeIdToString(type.InternalType()));
		}
	}
	case LogicalTypeId::ENUM: {
		switch (type.InternalType()) {
		case PhysicalType::UINT8:
			return Value::ENUM(reinterpret_cast<const uint8_t *>(data_ptr)[index], type);
		case PhysicalType::UINT16:
			return Value::ENUM(reinterpret_cast<const uint16_t *>(data_ptr)[index], type);
		case PhysicalType::UINT32:
			return Value::ENUM(reinterpret_cast<const uint32_t *>(data_ptr)[index], type);
		default:
			throw InternalException("ENUM can only have unsigned integers as physical types");
		}
	}
	case LogicalTypeId::POINTER:
		return Value::POINTER(reinterpret_cast<const uintptr_t *>(data_ptr)[index]);
	case LogicalTypeId::FLOAT:
		return Value::FLOAT(reinterpret_cast<const float *>(data_ptr)[index]);
	case LogicalTypeId::DOUBLE:
		return Value::DOUBLE(reinterpret_cast<const double *>(data_ptr)[index]);
	case LogicalTypeId::INTERVAL:
		return Value::INTERVAL(reinterpret_cast<const interval_t *>(data_ptr)[index]);
	case LogicalTypeId::VARCHAR: {
		auto str = reinterpret_cast<const string_t *>(data_ptr)[index];
		return Value(str.GetString());
	}
	case LogicalTypeId::BLOB: {
		auto str = reinterpret_cast<const string_t *>(data_ptr)[index];
		return Value::BLOB(const_data_ptr_cast(str.GetData()), str.GetSize());
	}
	case LogicalTypeId::BIGNUM: {
		auto str = reinterpret_cast<const bignum_t *>(data_ptr)[index];
		return Value::BIGNUM(const_data_ptr_cast(str.data.GetData()), str.data.GetSize());
	}
	case LogicalTypeId::GEOMETRY: {
		auto str = reinterpret_cast<const string_t *>(data_ptr)[index];
		if (GeoType::HasCRS(type)) {
			return Value::GEOMETRY(const_data_ptr_cast(str.GetData()), str.GetSize(), GeoType::GetCRS(type));
		}
		return Value::GEOMETRY(const_data_ptr_cast(str.GetData()), str.GetSize());
	}
	case LogicalTypeId::LEGACY_AGGREGATE_STATE: {
		auto str = reinterpret_cast<const string_t *>(data_ptr)[index];
		return Value::LEGACY_AGGREGATE_STATE(type, const_data_ptr_cast(str.GetData()), str.GetSize());
	}
	case LogicalTypeId::BIT: {
		auto str = reinterpret_cast<const string_t *>(data_ptr)[index];
		return Value::BIT(const_data_ptr_cast(str.GetData()), str.GetSize());
	}
	case LogicalTypeId::SQLNULL:
		return Value();
	case LogicalTypeId::TYPE: {
		auto blob = reinterpret_cast<const string_t *>(data_ptr)[index];
		return Value::TYPE(blob);
	}
	default:
		throw InternalException("Unimplemented type for StandardVectorBuffer::GetValue");
	}
}

void FlatVector::SetData(Vector &vector, data_ptr_t data, idx_t capacity) {
	VerifyFlatVector(vector);
	if (vector.GetType().InternalType() == PhysicalType::ARRAY) {
		throw InternalException("SetData not supported for array");
	}
	if (vector.GetType().InternalType() == PhysicalType::STRUCT) {
		throw InternalException("SetData not supported for struct");
	}
	// Preserve the validity mask from the old buffer before replacing it.
	// FIXME: this can maybe be removed in the future - it seems only the Arrow conversion code relies on this behavior
	auto old_validity = std::move(vector.BufferMutable().GetValidityMask());
	if (vector.GetType().InternalType() == PhysicalType::LIST) {
		auto &current_buffer = vector.BufferMutable().Cast<VectorListBuffer>();
		vector.SetBuffer(make_buffer<VectorListBuffer>(data, capacity, current_buffer.GetChild()));
	} else if (vector.GetType().InternalType() == PhysicalType::VARCHAR) {
		vector.SetBuffer(make_buffer<VectorStringBuffer>(data, capacity));
	} else {
		auto type_size = GetTypeIdSize(vector.GetType().InternalType());
		vector.SetBuffer(make_buffer<StandardVectorBuffer>(data, capacity, type_size));
	}
	vector.BufferMutable().GetValidityMask() = std::move(old_validity);
	// set the size of the new buffer so child->size() works for list parents using GetChildSize()
	vector.BufferMutable().SetVectorSize(capacity);
}

void FlatVector::SetNull(Vector &vector, idx_t idx, bool is_null) {
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
	vector.BufferMutable().GetValidityMask().Set(idx, !is_null);
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
		auto &child = ArrayVector::GetChildMutable(vector);
		auto array_size = ArrayType::GetSize(type);
		auto child_offset = idx * array_size;
		for (idx_t i = 0; i < array_size; i++) {
			FlatVector::SetNull(child, child_offset + i, is_null);
		}
	}
}

} // namespace duckdb
