#include "duckdb/common/vector/for_vector.hpp"

#include "duckdb/common/autovec.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// FOR metadata stored inline on VectorBuffer.
//===--------------------------------------------------------------------===//
PhysicalType FORVector::GetStoredType(const Vector &vector) {
	D_ASSERT(vector.GetVectorType() == VectorType::FOR_VECTOR);
	return vector.buffer->for_stored_type;
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

#if DUCKDB_AUTOVEC
// Zero-extend 8 values from src into dst; all loads complete before the stores, so overlapping in-place
// buffers are safe when processed back-to-front. Unsigned lanes are bit-identical to the signed widen.
template <class S, class L>
DUCKDB_AUTOVEC_TARGET static inline void WidenBlock8(const S *src, L *dst) {
	using namespace duckdb_bitpacking::internal;
	if constexpr (sizeof(S) == 1) {
		duckdb_bp_u8x8 v;
		std::memcpy(&v, src, 8);
		if constexpr (sizeof(L) == 2) {
			auto w = __builtin_convertvector(v, duckdb_bp_u16x8);
			std::memcpy(dst, &w, 16);
			return;
		}
		auto w = __builtin_convertvector(v, duckdb_bp_u32x8);
		if constexpr (sizeof(L) == 4) {
			std::memcpy(dst, &w, 32);
			return;
		}
		WidenBlock8<uint32_t, uint64_t>(reinterpret_cast<const uint32_t *>(&w), reinterpret_cast<uint64_t *>(dst));
	} else if constexpr (sizeof(S) == 2) {
		duckdb_bp_u16x8 v;
		std::memcpy(&v, src, 16);
		auto w = __builtin_convertvector(v, duckdb_bp_u32x8);
		if constexpr (sizeof(L) == 4) {
			std::memcpy(dst, &w, 32);
			return;
		}
		WidenBlock8<uint32_t, uint64_t>(reinterpret_cast<const uint32_t *>(&w), reinterpret_cast<uint64_t *>(dst));
	} else {
		duckdb_bp_u32x8 v;
		std::memcpy(&v, src, 32);
		auto lo = __builtin_convertvector(__builtin_shufflevector(v, v, 0, 1, 2, 3), duckdb_bp_u64x4);
		auto hi = __builtin_convertvector(__builtin_shufflevector(v, v, 4, 5, 6, 7), duckdb_bp_u64x4);
		std::memcpy(dst + 0, &lo, 32);
		std::memcpy(dst + 4, &hi, 32);
	}
}
#endif

template <class LOGICAL_T>
static void WidenFORPayload(PhysicalType stored_type, const_data_ptr_t source, data_ptr_t target,
                            const SelectionVector &sel, idx_t count) {
	auto target_data = reinterpret_cast<LOGICAL_T *>(target);
	FOR_SWITCH_STORED(stored_type, STORED_T, {
		auto source_data = reinterpret_cast<const STORED_T *>(source);
		idx_t i = 0;
#if DUCKDB_AUTOVEC
		if constexpr (sizeof(LOGICAL_T) <= 8 && sizeof(STORED_T) < sizeof(LOGICAL_T)) {
			if (!sel.IsSet()) {
				// dense case: disjoint buffers, widen forward in 8-value blocks
				using UL = typename MakeUnsigned<LOGICAL_T>::type;
				auto udst = reinterpret_cast<UL *>(target);
				for (; i + 8 <= count; i += 8) {
					WidenBlock8<STORED_T, UL>(source_data + i, udst + i);
				}
			}
		}
#endif
		for (; i < count; i++) {
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
	const auto stored_type = buffer.for_stored_type;
	buffer.SetVectorTypeOnly(VectorType::FLAT_VECTOR);
	FOR_SWITCH_LOGICAL(type.InternalType(), LOGICAL_T, {
		FOR_SWITCH_STORED(stored_type, STORED_T, {
			auto src = reinterpret_cast<const STORED_T *>(data);
			auto dst = reinterpret_cast<LOGICAL_T *>(data);
			if constexpr (sizeof(STORED_T) >= sizeof(LOGICAL_T)) {
				// payload is already the logical width bit-for-bit
				(void)src;
				(void)dst;
			} else {
				bool done = false;
#if DUCKDB_AUTOVEC
				if constexpr (sizeof(LOGICAL_T) <= 8) {
					// widen back-to-front in 8-value blocks: each block loads before it stores
					using UL = typename MakeUnsigned<LOGICAL_T>::type;
					auto udst = reinterpret_cast<UL *>(data);
					idx_t i = count & ~idx_t(7);
					for (idx_t k = count; k-- > i;) {
						dst[k] = WidenStored<LOGICAL_T>(src[k]);
					}
					while (i) {
						i -= 8;
						WidenBlock8<STORED_T, UL>(src + i, udst + i);
					}
					done = true;
				}
#endif
				if (!done) {
					// widen back-to-front in chunks via a stack copy so the loop has disjoint operands
					STORED_T tmp[STANDARD_VECTOR_SIZE];
					for (idx_t end = count; end > 0;) {
						const idx_t base = end > STANDARD_VECTOR_SIZE ? end - STANDARD_VECTOR_SIZE : 0;
						const idx_t n = end - base;
						memcpy(tmp, src + base, n * sizeof(STORED_T));
						for (idx_t i = 0; i < n; i++) {
							dst[base + i] = WidenStored<LOGICAL_T>(tmp[i]);
						}
						end = base;
					}
				}
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
			if (FORVector::GetStoredType(target) != FORVector::GetStoredType(source)) {
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

// casts whose payload is bit-identical: integral <-> integral, integer -> scale-0 decimal, same-scale decimal widening
static bool IsPayloadIdentityCast(const LogicalType &src, const LogicalType &dst) {
	if (IsPlainIntegral(src)) {
		return IsPlainIntegral(dst) || (dst.id() == LogicalTypeId::DECIMAL && DecimalType::GetScale(dst) == 0);
	}
	if (src.id() == LogicalTypeId::DECIMAL && dst.id() == LogicalTypeId::DECIMAL) {
		return DecimalType::GetScale(src) == DecimalType::GetScale(dst) &&
		       DecimalType::GetWidth(dst) >= DecimalType::GetWidth(src);
	}
	return false;
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
	// only payload-identity casts qualify; anything with conversion semantics
	// (decimal rescaling, date/timestamp, enum) must run the real cast
	if (!IsPayloadIdentityCast(source.GetType(), result.GetType())) {
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
	// decimal targets additionally bound the value domain by their width
	if (result.GetType().id() == LogicalTypeId::DECIMAL &&
	    max_storage >= Uhugeint::POWERS_OF_TEN[DecimalType::GetWidth(result.GetType())]) {
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
