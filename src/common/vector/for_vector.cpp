#include "duckdb/common/vector/for_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// FOR metadata stored inline on VectorBuffer.
//===--------------------------------------------------------------------===//
PhysicalType FORVector::GetStoredType(const Vector &vector) {
	D_ASSERT(vector.GetVectorType() == VectorType::FOR_VECTOR);
	return vector.buffer->for_stored_type;
}

uint8_t FORVector::GetRangeBits(const Vector &vector) {
	auto delta = vector.buffer->for_max_value;
	uint8_t result = 0;
	while (delta != 0) {
		result++;
		delta >>= 1;
	}
	return result;
}

template <class T>
T FORVector::GetMax(const Vector &vector) {
	D_ASSERT(vector.GetVectorType() == VectorType::FOR_VECTOR);
	return FORValueOps<T>::FromUnsignedStorage(vector.buffer->for_max_value);
}

template <class T>
void FORVector::SetMetadata(Vector &vector, PhysicalType stored_type, T max_value) {
	D_ASSERT(vector.GetVectorType() == VectorType::FOR_VECTOR);
	vector.buffer->for_stored_type = stored_type;
	vector.buffer->for_max_value = FORValueOps<T>::ToUnsignedStorage(max_value);
}

// Explicit instantiations
#define FOR_INSTANTIATE(T)                                                                                             \
	template T FORVector::GetMax<T>(const Vector &);                                                                   \
	template void FORVector::SetMetadata<T>(Vector &, PhysicalType, T);                                                \
	template bool FORVector::TryReferencePayload<T>(Vector &, Vector &, PhysicalType, T, idx_t);                       \
	template void FORVector::Create<T>(Vector &, PhysicalType, T);
FOR_INSTANTIATE(int16_t)
FOR_INSTANTIATE(int32_t)
FOR_INSTANTIATE(int64_t)
FOR_INSTANTIATE(hugeint_t)
FOR_INSTANTIATE(uint16_t)
FOR_INSTANTIATE(uint32_t)
FOR_INSTANTIATE(uint64_t)
FOR_INSTANTIATE(uhugeint_t)
#undef FOR_INSTANTIATE

//===--------------------------------------------------------------------===//
// Shared helpers
//===--------------------------------------------------------------------===//
LogicalType FORVector::StoredTypeToLogical(PhysicalType stored_type) {
	switch (stored_type) {
	case PhysicalType::UINT8:
		return LogicalType::UTINYINT;
	case PhysicalType::UINT16:
		return LogicalType::USMALLINT;
	case PhysicalType::UINT32:
		return LogicalType::UINTEGER;
	case PhysicalType::UINT64:
		return LogicalType::UBIGINT;
	default:
		throw InternalException("Unsupported stored type for FOR vector");
	}
}

Vector FORVector::CreateStoredView(const Vector &for_vec) {
	D_ASSERT(for_vec.GetVectorType() == VectorType::FOR_VECTOR);
	auto stored_data = const_cast<data_ptr_t>(FORVector::GetData(for_vec));
	Vector stored_vec(StoredTypeToLogical(FORVector::GetStoredType(for_vec)), stored_data, for_vec.size());
	stored_vec.BufferMutable().AddAuxiliaryData(make_uniq<VectorBufferHolder>(for_vec.GetBufferRef()));
	stored_vec.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetValidity(stored_vec, FORVector::Validity(for_vec));
	return stored_vec;
}

Vector FORVector::CreatePayloadView(PhysicalType stored_type, data_ptr_t payload, idx_t count) {
	Vector payload_vec(StoredTypeToLogical(stored_type), payload, count);
	payload_vec.SetVectorType(VectorType::FLAT_VECTOR);
	return payload_vec;
}

//===--------------------------------------------------------------------===//
// Decompress
//===--------------------------------------------------------------------===//
template <class LOGICAL_T>
static void DecompressImpl(const Vector &source, Vector &target, idx_t count, const SelectionVector *sel = nullptr) {
	auto src = FORVector::GetData(source);
	auto dst = FlatVector::GetDataMutable(target);
	FOR_SWITCH_STORED(FORVector::GetStoredType(source), S, {
		auto stored = reinterpret_cast<const S *>(src);
		auto target_data = reinterpret_cast<LOGICAL_T *>(dst);
		for (idx_t i = 0; i < count; i++) {
			target_data[i] = FORVector::WidenStored<LOGICAL_T>(stored[sel ? sel->get_index(i) : i]);
		}
	});
}

void FORVector::Decompress(const Vector &source, Vector &target, idx_t count) {
	D_ASSERT(source.GetVectorType() == VectorType::FOR_VECTOR);
	FOR_SWITCH_LOGICAL(source.GetType().InternalType(), T, { DecompressImpl<T>(source, target, count); });
}
void FORVector::Decompress(const Vector &source, Vector &target, const SelectionVector &sel, idx_t count) {
	D_ASSERT(source.GetVectorType() == VectorType::FOR_VECTOR);
	FOR_SWITCH_LOGICAL(source.GetType().InternalType(), T, { DecompressImpl<T>(source, target, count, &sel); });
}

template <class LOGICAL_T>
static void WidenFORPayload(PhysicalType stored_type, const_data_ptr_t source, data_ptr_t target,
                            const SelectionVector &sel, idx_t count) {
	auto target_data = reinterpret_cast<LOGICAL_T *>(target);
	FOR_SWITCH_STORED(stored_type, STORED_T, {
		auto source_data = reinterpret_cast<const STORED_T *>(source);
		for (idx_t i = 0; i < count; i++) {
			target_data[i] = FORVector::WidenStored<LOGICAL_T>(source_data[sel.get_index(i)]);
		}
	});
}

void FORVector::WidenToFlat(const LogicalType &type, PhysicalType stored_type, const_data_ptr_t source,
                            data_ptr_t target, const SelectionVector &sel, idx_t count) {
	FOR_SWITCH_LOGICAL(type.InternalType(), LOGICAL_T,
	                   { WidenFORPayload<LOGICAL_T>(stored_type, source, target, sel, count); });
}

void FORVector::CopyToFlat(const Vector &source, const SelectionVector &sel, Vector &target, idx_t source_offset,
                           idx_t target_offset, idx_t copy_count) {
	D_ASSERT(source.GetVectorType() == VectorType::FOR_VECTOR);
	D_ASSERT(target.GetVectorType() == VectorType::FLAT_VECTOR);
	auto st = GetStoredType(source);
	auto *src = GetData(source);
	FOR_SWITCH_LOGICAL(source.GetType().InternalType(), LT, {
		auto *dst = FlatVector::GetDataMutable<LT>(target);
		FOR_SWITCH_STORED(st, ST, {
			auto *s = reinterpret_cast<const ST *>(src);
			for (idx_t i = 0; i < copy_count; i++) {
				dst[target_offset + i] = WidenStored<LT>(s[sel.get_index(source_offset + i)]);
			}
		});
	});
}

template <class LOGICAL_T>
static LOGICAL_T GetFORValue(PhysicalType stored_type, const_data_ptr_t data, idx_t index) {
	LOGICAL_T result;
	FOR_SWITCH_STORED(stored_type, STORED_T,
	                  { result = FORVector::WidenStored<LOGICAL_T>(reinterpret_cast<const STORED_T *>(data)[index]); });
	return result;
}

Value FORVector::GetValue(const LogicalType &type, PhysicalType stored_type, const_data_ptr_t data, idx_t index) {
	switch (type.InternalType()) {
	case PhysicalType::INT16:
		return Value::SMALLINT(GetFORValue<int16_t>(stored_type, data, index));
	case PhysicalType::INT32:
		return Value::INTEGER(GetFORValue<int32_t>(stored_type, data, index));
	case PhysicalType::INT64:
		return Value::BIGINT(GetFORValue<int64_t>(stored_type, data, index));
	case PhysicalType::INT128:
		return type.id() == LogicalTypeId::UUID ? Value::UUID(GetFORValue<hugeint_t>(stored_type, data, index))
		                                        : Value::HUGEINT(GetFORValue<hugeint_t>(stored_type, data, index));
	case PhysicalType::UINT16:
		return Value::USMALLINT(GetFORValue<uint16_t>(stored_type, data, index));
	case PhysicalType::UINT32:
		return Value::UINTEGER(GetFORValue<uint32_t>(stored_type, data, index));
	case PhysicalType::UINT64:
		return Value::UBIGINT(GetFORValue<uint64_t>(stored_type, data, index));
	case PhysicalType::UINT128:
		return Value::UHUGEINT(GetFORValue<uhugeint_t>(stored_type, data, index));
	default:
		throw InternalException("Unsupported FOR vector logical type");
	}
}

//===--------------------------------------------------------------------===//
// Create
//===--------------------------------------------------------------------===//
template <class T>
void FORVector::Create(Vector &vector, PhysicalType stored_type, T max_value) {
	D_ASSERT(vector.buffer);
	D_ASSERT(vector.buffer->GetData());
	vector.buffer->SetVectorTypeOnly(VectorType::FOR_VECTOR);
	vector.buffer->SetVectorSizeOnly(vector.buffer->Capacity());
	vector.buffer->GetValidityMask().Reset();
	FORVector::SetMetadata<T>(vector, stored_type, max_value);
}

void FORVector::PrepareAppend(Vector &target, const Vector &source, bool has_selection, idx_t target_size) {
	if (!has_selection && source.GetVectorType() == VectorType::FOR_VECTOR) {
		if (target.GetVectorType() == VectorType::FOR_VECTOR) {
			if (!FORVector::HasSameMetadata(target, source)) {
				target.Flatten();
			}
		} else if (target_size == 0) {
			target.BufferMutable().SetVectorTypeOnly(VectorType::FOR_VECTOR);
			FOR_SWITCH_LOGICAL(target.GetType().InternalType(), LOGICAL_T, {
				FORVector::SetMetadata<LOGICAL_T>(target, FORVector::GetStoredType(source),
				                                  FORVector::GetMax<LOGICAL_T>(source));
			});
		}
	} else if (target.GetVectorType() == VectorType::FOR_VECTOR) {
		target.Flatten();
	}
}

template <class T>
bool FORVector::TryReferencePayload(Vector &source, Vector &result, PhysicalType stored_type, T max_value,
                                    idx_t count) {
	if (source.GetVectorType() != VectorType::FLAT_VECTOR) {
		return false;
	}
	if (source.GetType().InternalType() != stored_type) {
		return false;
	}
	auto data = FlatVector::GetDataMutable(source);
	auto buffer = make_buffer<StandardVectorBuffer>(data, count_t(count), GetTypeIdSize(stored_type));
	buffer->SetVectorTypeOnly(VectorType::FOR_VECTOR);
	buffer->AddAuxiliaryData(make_uniq<VectorBufferHolder>(source.GetBufferRef()));
	result.SetBuffer(std::move(buffer));
	FORVector::SetMetadata<T>(result, stored_type, max_value);
	FORVector::Validity(result).Initialize(FlatVector::Validity(source));
	return true;
}

template <class SRC_T, class DST_T>
static bool TryCastFORMetadata(Vector &source, uhugeint_t &max_storage) {
	DST_T max_value;
	if (!TryCast::Operation(FORVector::GetMax<SRC_T>(source), max_value)) {
		return false;
	}
	max_storage = FORValueOps<DST_T>::ToUnsignedStorage(max_value);
	return true;
}

bool FORVector::TryWidenType(Vector &source, Vector &result) {
	if (source.GetVectorType() != VectorType::FOR_VECTOR) {
		return false;
	}
	if (GetTypeIdSize(result.GetType().InternalType()) < GetTypeIdSize(source.GetType().InternalType())) {
		return false;
	}
	auto st = GetStoredType(source);
	uhugeint_t max_storage;
	bool success = false;
	FOR_SWITCH_LOGICAL(source.GetType().InternalType(), SRC_T, {
		FOR_SWITCH_LOGICAL(result.GetType().InternalType(), DST_T,
		                   { success = TryCastFORMetadata<SRC_T, DST_T>(source, max_storage); });
	});
	if (!success) {
		return false;
	}

	result.SetBuffer(source.GetBufferRef());
	result.GetBufferRef()->SetVectorTypeOnly(VectorType::FOR_VECTOR);
	FORVector::Validity(result).Initialize(Validity(source));
	result.buffer->for_stored_type = st;
	result.buffer->for_max_value = max_storage;
	return true;
}

} // namespace duckdb
