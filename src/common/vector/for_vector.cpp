#include "duckdb/common/vector/for_vector.hpp"

#include "duckdb/common/vector/dictionary_vector.hpp"
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

Vector FORVector::CreatePayloadView(PhysicalType stored_type, data_ptr_t payload, idx_t count) {
	Vector payload_vec(StoredTypeToLogical(stored_type), payload, count);
	payload_vec.SetVectorType(VectorType::FLAT_VECTOR);
	return payload_vec;
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

void FORVector::WidenInPlace(const LogicalType &type, VectorBuffer &buffer) {
	D_ASSERT(buffer.GetVectorType() == VectorType::FOR_VECTOR);
	auto data = buffer.GetData();
	auto count = buffer.Size();
	FOR_SWITCH_LOGICAL(type.InternalType(), LOGICAL_T, {
		FOR_SWITCH_STORED(buffer.for_stored_type, STORED_T, {
			auto src = reinterpret_cast<const STORED_T *>(data);
			auto dst = reinterpret_cast<LOGICAL_T *>(data);
			for (idx_t i = count; i-- > 0;) {
				dst[i] = WidenStored<LOGICAL_T>(src[i]);
			}
		});
	});
	buffer.SetVectorTypeOnly(VectorType::FLAT_VECTOR);
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

template <class SRC_T, class DST_T>
static bool TryCastFORMetadata(Vector &source, uhugeint_t &max_storage) {
	DST_T max_value;
	if (!TryCast::Operation(FORVector::GetMax<SRC_T>(source), max_value)) {
		return false;
	}
	max_storage = FORValueOps<DST_T>::ToUnsignedStorage(max_value);
	return true;
}

static bool IsPlainIntegral(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT:
		return true;
	default:
		return false;
	}
}

bool FORVector::TryCastType(Vector &source, Vector &result, idx_t count) {
	if (source.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(source);
		Vector retyped(result.GetType(), buffer_ptr<VectorBuffer>());
		if (!TryCastType(child, retyped, child.size())) {
			return false;
		}
		auto entry = make_buffer<DictionaryEntry>(std::move(retyped));
		result.Dictionary(std::move(entry), DictionaryVector::SelVector(source), count);
		return true;
	}
	if (source.GetVectorType() != VectorType::FOR_VECTOR) {
		return false;
	}
	// only plain integral <-> integral casts are the identity on the payload; anything with
	// conversion semantics (decimal rescaling, date/timestamp, enum) must run the real cast
	if (!IsPlainIntegral(source.GetType()) || !IsPlainIntegral(result.GetType())) {
		return false;
	}
	auto st = GetStoredType(source);
	if (GetTypeIdSize(st) > GetTypeIdSize(result.GetType().InternalType())) {
		return false;
	}
	// the cast can never fail if the FOR max fits the target type (all values are in [0, max])
	uhugeint_t max_storage;
	bool success = false;
	FOR_SWITCH_LOGICAL(source.GetType().InternalType(), SRC_T, {
		FOR_SWITCH_LOGICAL(result.GetType().InternalType(), DST_T,
		                   { success = TryCastFORMetadata<SRC_T, DST_T>(source, max_storage); });
	});
	if (!success) {
		return false;
	}

	auto buffer = make_buffer<StandardVectorBuffer>(GetData(source), count_t(count),
	                                                GetTypeIdSize(result.GetType().InternalType()));
	buffer->AddAuxiliaryData(make_uniq<VectorBufferHolder>(source.GetBufferRef()));
	if (GetTypeIdSize(st) == GetTypeIdSize(result.GetType().InternalType())) {
		// stored payload is already the target width bit-for-bit (all values in [0, max] fit the target),
		// so the FOR label adds nothing: hand back a FLAT reinterpret. This is what compressed
		// materialization wants - a narrow flat key that hashes/scatters with no flatten pass.
		buffer->SetVectorTypeOnly(VectorType::FLAT_VECTOR);
		result.SetBuffer(std::move(buffer));
		FlatVector::SetValidity(result, Validity(source));
		return true;
	}
	// still narrower than the target: keep it as a FOR view over the same payload (a separate buffer so
	// flattening the result can never mutate data seen through the source vector)
	buffer->SetVectorTypeOnly(VectorType::FOR_VECTOR);
	buffer->for_stored_type = st;
	buffer->for_max_value = max_storage;
	result.SetBuffer(std::move(buffer));
	FORVector::Validity(result).Initialize(Validity(source));
	return true;
}

} // namespace duckdb
