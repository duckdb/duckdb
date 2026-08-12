#include "duckdb/common/vector/for_vector.hpp"

#include "duckdb/common/autovec.hpp"
namespace duckdb {

void ForVector::Create(Vector &vector, PhysicalType stored_type, uint64_t max_stored, idx_t count) {
	auto &buffer = vector.BufferMutable();
	D_ASSERT(buffer.GetData());
	D_ASSERT(buffer.cache_owned); // in-place widening needs the full-stride allocation
	buffer.for_count = count;
	buffer.SetVectorTypeOnly(VectorType::FOR_VECTOR);
	buffer.for_stored_type = stored_type;
	buffer.for_max = max_stored;
	buffer.for_exploited = false;
}

bool ForVector::TryRetype(Vector &source, Vector &result, idx_t count) {
	// only a plain integer narrowing reinterprets the payload as-is; decimal rescaling, dates and enums do not
	if (!IsFor(source) || !source.GetType().IsIntegral() || !result.GetType().IsIntegral()) {
		return false;
	}
	const auto target_size = GetTypeIdSize(result.GetType().InternalType());
	if (GetTypeIdSize(StoredType(source)) != target_size || target_size > 4) {
		return false; // only an exact-width handoff avoids the copy
	}
	// every value is in [0, for_max], so the cast cannot fail once the target holds for_max
	const uint64_t target_max = result.GetType().IsSigned() ? (uint64_t(1) << (target_size * 8 - 1)) - 1
	                                                        : (uint64_t(1) << (target_size * 8)) - 1;
	if (MaxStored(source) > target_max) {
		return false;
	}
	auto buffer = make_buffer<StandardVectorBuffer>(source.BufferMutable().GetData(), count_t(count), target_size);
	buffer->GetValidityMask().Initialize(source.Buffer().GetValidityMask());
	buffer->AddAuxiliaryData(make_uniq<VectorBufferHolder>(source.GetBufferRef()));
	// the view aliases the payload, so the source must lose its in-place widen permission
	source.BufferMutable().cache_owned = false;
	MarkExploited(source); // the retype used the narrow payload: keep the scan producing FOR for this column
	result.SetBuffer(std::move(buffer));
	return true;
}

//! Copy the payload narrow while proving its bounds: half the writes of the widening cast, and the exact for_max.
template <class T>
DUCKDB_AUTOVEC_TARGET static bool PromoteLoop(const T *DUCKDB_BITPACKING_RESTRICT src,
                                              T *DUCKDB_BITPACKING_RESTRICT dst, idx_t count, uint64_t &max_out) {
	T lo = 0, hi = 0;
	DUCKDB_UNROLL_LOOP
	for (idx_t i = 0; i < count; i++) {
		dst[i] = src[i];
		lo = MinValue(lo, src[i]);
		hi = MaxValue(hi, src[i]);
	}
	max_out = static_cast<uint64_t>(hi);
	return lo >= 0; // a negative value has no unsigned narrow representation
}

bool ForVector::TryPromote(Vector &source, Vector &result, idx_t cnt) {
	const auto src_size = GetTypeIdSize(source.GetType().InternalType());
	const auto dst_size = GetTypeIdSize(result.GetType().InternalType());
	auto &buffer = result.GetBufferRef();
	if (!source.GetType().IsIntegral() || !result.GetType().IsIntegral() || src_size > 4 || dst_size > 8 ||
	    src_size >= dst_size || source.GetVectorType() != VectorType::FLAT_VECTOR || !buffer || !buffer->cache_owned ||
	    buffer->Capacity() < cnt || !TokenSet(result)) {
		return false;
	}
	auto src = FlatVector::GetDataUnsafe(source);
	auto dst = buffer->GetData();
	uint64_t lim;
	auto promote = [&](auto t) {
		using T = decltype(t);
		return PromoteLoop(reinterpret_cast<const T *>(src), reinterpret_cast<T *>(dst), cnt, lim);
	};
	const bool fits = src_size == 1 ? promote(int8_t(0)) : src_size == 2 ? promote(int16_t(0)) : promote(int32_t(0));
	if (!fits) {
		return false; // the real cast overwrites the partial narrow copy
	}
	FlatVector::SetValidity(result, source.Buffer().GetValidityMask());
	auto type = src_size == 1 ? PhysicalType::UINT8 : (src_size == 2 ? PhysicalType::UINT16 : PhysicalType::UINT32);
	Create(result, type, lim, cnt);
	return true;
}

//! A constant above the payload's ceiling makes every comparison uniform
static bool ClampStoredConstant(const Vector &vector, uint64_t &value) {
	const auto max_stored = ForVector::MaxStored(vector);
	if (value <= max_stored) {
		return true;
	}
	const auto stored_size = GetTypeIdSize(ForVector::StoredType(vector));
	const uint64_t stored_max =
	    stored_size == 8 ? NumericLimits<uint64_t>::Maximum() : ((uint64_t(1) << (stored_size * 8)) - 1);
	if (max_stored >= stored_max) {
		return false; // no headroom for the sentinel
	}
	value = max_stored + 1;
	return true;
}

bool ForVector::TryStoredConstant(const Vector &vector, const Value &constant, uint64_t &result) {
	int64_t value;
	switch (vector.GetType().InternalType()) {
	case PhysicalType::INT16:
		value = constant.GetValueUnsafe<int16_t>();
		break;
	case PhysicalType::INT32:
		value = constant.GetValueUnsafe<int32_t>();
		break;
	case PhysicalType::INT64:
		value = constant.GetValueUnsafe<int64_t>();
		break;
	case PhysicalType::UINT16:
		value = constant.GetValueUnsafe<uint16_t>();
		break;
	case PhysicalType::UINT32:
		value = constant.GetValueUnsafe<uint32_t>();
		break;
	case PhysicalType::UINT64:
		result = constant.GetValueUnsafe<uint64_t>();
		return ClampStoredConstant(vector, result);
	default:
		return false;
	}
	if (value < 0) {
		return false; // no unsigned stored constant expresses "below every value"
	}
	result = static_cast<uint64_t>(value);
	return ClampStoredConstant(vector, result);
}

//===--------------------------------------------------------------------===//
// Widen - the only execution loop FOR owns
//===--------------------------------------------------------------------===//
//! Back-to-front so a wider destination can overwrite its own narrower source
template <class SRC, class DST>
DUCKDB_AUTOVEC_TARGET static void WidenInPlaceLoop(data_ptr_t data, idx_t count) {
	auto src = reinterpret_cast<const SRC *>(data);
	auto dst = reinterpret_cast<DST *>(data);
	for (idx_t i = count; i-- > 0;) {
		dst[i] = static_cast<DST>(src[i]);
	}
}

//! Route a (stored, target) width pair to fun(SRC{}, DST{})
template <class FUNC>
static void DispatchWiden(PhysicalType stored, PhysicalType target, FUNC &&fun) {
	const auto src = GetTypeIdSize(stored);
	auto with_src = [&](auto dst) {
		return src == 1   ? fun(uint8_t(0), dst)
		       : src == 2 ? fun(uint16_t(0), dst)
		       : src == 4 ? fun(uint32_t(0), dst)
		                  : fun(uint64_t(0), dst);
	};
	const auto tgt = GetTypeIdSize(target);
	return tgt == 2 ? with_src(uint16_t(0)) : tgt == 4 ? with_src(uint32_t(0)) : with_src(uint64_t(0));
}

void ForVector::WidenInPlace(data_ptr_t data, PhysicalType stored, PhysicalType target_type, idx_t count) {
	if (GetTypeIdSize(stored) == GetTypeIdSize(target_type)) {
		return; // a full-width payload only carries the max: dropping the flag is the whole widen
	}
	DispatchWiden(stored, target_type,
	              [&](auto s, auto d) { WidenInPlaceLoop<decltype(s), decltype(d)>(data, count); });
}

template <class SRC, class DST>
DUCKDB_AUTOVEC_TARGET static void WidenGatherLoop(const_data_ptr_t src_p, data_ptr_t target, const SelectionVector &sel,
                                                  idx_t count, idx_t sel_offset) {
	auto src = reinterpret_cast<const SRC *>(src_p);
	auto dst = reinterpret_cast<DST *>(target);
	for (idx_t i = 0; i < count; i++) {
		dst[i] = static_cast<DST>(src[sel.get_index(sel_offset + i)]);
	}
}

void ForVector::WidenGather(const_data_ptr_t src, PhysicalType stored, data_ptr_t target, PhysicalType target_type,
                            const SelectionVector &sel, idx_t count, idx_t sel_offset) {
	DispatchWiden(stored, target_type, [&](auto s, auto d) {
		WidenGatherLoop<decltype(s), decltype(d)>(src, target, sel, count, sel_offset);
	});
}

void ForVector::WidenInPlace(const LogicalType &type, VectorBuffer &buffer) {
	WidenInPlace(buffer.GetData(), buffer.for_stored_type, type.InternalType(), buffer.for_count);
	if (!buffer.for_exploited) {
		// nothing used the narrow payload: sit out a cooldown before producing it again
		buffer.for_cooldown = COOLDOWN;
	}
	buffer.SetVectorTypeOnly(VectorType::FLAT_VECTOR);
	buffer.for_stored_type = PhysicalType::INVALID;
}

} // namespace duckdb
