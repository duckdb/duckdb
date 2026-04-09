#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/array_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/types/bignum.hpp"

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


buffer_ptr<VectorBuffer> StandardVectorBuffer::Slice(const LogicalType &type, const VectorBuffer &source, idx_t offset,
                                                      idx_t end) {
	auto &src = source.Cast<const StandardVectorBuffer>();
	auto type_size = GetTypeIdSize(type.InternalType());
	auto offset_ptr = src.data_ptr + type_size * offset;
	auto result = make_buffer<StandardVectorBuffer>(offset_ptr);
	result->GetValidityMask().Slice(src.validity, offset, end - offset);
	return result;
}

void StandardVectorBuffer::ToUnifiedFormat(const Vector &vector, idx_t count, UnifiedVectorFormat &format) const {
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		format.sel = ConstantVector::ZeroSelectionVector(count, format.owned_sel);
	} else {
		format.sel = FlatVector::IncrementalSelectionVector();
	}
	format.data = data_ptr;
	format.validity = validity;
}

void StandardVectorBuffer::SetValue(const LogicalType &type, idx_t index, const Value &val) {
	if (!val.IsNull() && val.type() != type) {
		SetValue(type, index, val.DefaultCastAs(type));
		return;
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

buffer_ptr<VectorBuffer> StandardVectorBuffer::Flatten(const LogicalType &type, const SelectionVector &sel,
                                                       idx_t count) {
	if (!sel.IsSet() && vector_type == VectorType::FLAT_VECTOR) {
		return nullptr;
	}
	// determine the selection vector to use
	SelectionVector owned_sel;
	const SelectionVector *active_sel = &sel;
	if (!sel.IsSet()) {
		D_ASSERT(vector_type == VectorType::CONSTANT_VECTOR);
		active_sel = ConstantVector::ZeroSelectionVector(count, owned_sel);
	}
	auto flat_count = MaxValue<idx_t>(STANDARD_VECTOR_SIZE, count);
	auto type_size = GetTypeIdSize(type.InternalType());
	auto result = make_buffer<StandardVectorBuffer>(flat_count, type_size);
	// copy data using sel
	auto dst = result->GetData();
	for (idx_t i = 0; i < count; i++) {
		auto src_idx = active_sel->get_index(i);
		memcpy(dst + i * type_size, data_ptr + src_idx * type_size, type_size);
	}
	// copy validity using sel
	auto &result_validity = result->GetValidityMask();
	result_validity.CopySel(validity, *active_sel, 0, 0, count);
	return result;
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
