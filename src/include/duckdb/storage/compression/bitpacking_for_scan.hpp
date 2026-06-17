//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/bitpacking_for_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/bitpacking.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/vector/for_vector.hpp"

#include <algorithm>
#include <cstring>

namespace duckdb {

template <class T, class T_S = typename MakeSigned<T>::type>
struct BitpackingScanState;

template <class T>
static void ApplyFrameOfReference(T *__restrict dst, T frame_of_reference, idx_t size) {
	using T_U = typename MakeUnsigned<T>::type;
	if (!frame_of_reference) {
		return;
	}
	auto *__restrict data = reinterpret_cast<T_U *>(dst);
	auto frame = static_cast<T_U>(frame_of_reference);
	for (idx_t i = 0; i < size; i++) {
		data[i] += frame;
	}
}

template <class T>
static void DecodeFORBlocksBatchFlat(BitpackingScanState<T> &scan_state, idx_t group_offset, idx_t count,
                                     T *result_ptr) {
	D_ASSERT(count % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE == 0);
	D_ASSERT(group_offset % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE == 0);
	auto width = scan_state.current_width;
	data_ptr_t src = scan_state.current_group_ptr + group_offset * width / 8;
	BitpackingPrimitives::UnPackBuffer<T>(data_ptr_cast(result_ptr), src, count, width, true);
	ApplyFrameOfReference<T>(result_ptr, scan_state.current_frame_of_reference, count);
}

template <class T>
static bool TryFORStoredTypeForMax(T max_value, PhysicalType &stored_type) {
	if (NumericLimits<T>::IsSigned() && max_value < T(0)) {
		return false;
	}
	if (!FORVector::TryGetStoredTypeForMax<T>(max_value, stored_type)) {
		return false;
	}
	return GetTypeIdSize(stored_type) < sizeof(T);
}

template <class T>
static bool TryFORGroupType(T frame_of_reference, bitpacking_width_t width, PhysicalType &stored_type, T &group_max) {
	if ((NumericLimits<T>::IsSigned() && frame_of_reference < T(0)) || width >= 64) {
		return false;
	}
	auto max_delta = width == 0 ? uint64_t(0) : (uint64_t(1) << width) - 1;
	T typed_delta;
	return TryCast::Operation<uint64_t, T>(max_delta, typed_delta) &&
	       TryAddOperator::Operation(frame_of_reference, typed_delta, group_max) &&
	       TryFORStoredTypeForMax<T>(group_max, stored_type);
}

static void WidenFORData(data_ptr_t buf, idx_t count, PhysicalType old_type, PhysicalType new_type) {
	FOR_SWITCH_STORED(old_type, OLD, {
		FOR_SWITCH_STORED(new_type, NEW, {
			auto src = reinterpret_cast<const OLD *>(buf);
			auto dst = reinterpret_cast<NEW *>(buf);
			for (idx_t j = count; j-- > 0;) {
				dst[j] = static_cast<NEW>(src[j]);
			}
		});
	});
}

template <class T>
static void FORToFlat(data_ptr_t buf, idx_t count, PhysicalType stored_type) {
	auto dst = reinterpret_cast<T *>(buf);
	FOR_SWITCH_STORED(stored_type, S, {
		auto src = reinterpret_cast<const S *>(buf);
		for (idx_t j = count; j-- > 0;) {
			dst[j] = FORVector::WidenStored<T>(src[j]);
		}
	});
}

static void ReconcileFORType(data_ptr_t result_buf, idx_t scanned, PhysicalType requested_type, PhysicalType &for_st) {
	if (for_st == PhysicalType::INVALID) {
		for_st = requested_type;
		return;
	}
	if (for_st != requested_type && GetTypeIdSize(requested_type) > GetTypeIdSize(for_st)) {
		WidenFORData(result_buf, scanned, for_st, requested_type);
		for_st = requested_type;
	}
}

template <class T>
static void FillFORConstant(data_ptr_t result_buf, idx_t offset, idx_t count, PhysicalType stored_type, T value) {
	FOR_SWITCH_STORED(stored_type, ST, {
		auto target = reinterpret_cast<ST *>(result_buf + offset * GetTypeIdSize(stored_type));
		std::fill(target, target + count, UnsafeNumericCast<ST>(value));
	});
}

template <class T>
static void DecodeFORGroupsBatch(BitpackingScanState<T> &scan_state, idx_t group_offset, idx_t count,
                                 data_ptr_t result_buf, idx_t narrow_offset, PhysicalType stored_type) {
	D_ASSERT(count % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE == 0);
	D_ASSERT(group_offset % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE == 0);
	auto width = scan_state.current_width;
	data_ptr_t src = scan_state.current_group_ptr + group_offset * width / 8;
	data_ptr_t dst = result_buf + narrow_offset * GetTypeIdSize(stored_type);
	FOR_SWITCH_STORED(stored_type, ST, {
		BitpackingPrimitives::UnPackBuffer<ST>(dst, src, count, width, true);
		ApplyFrameOfReference<ST>(reinterpret_cast<ST *>(dst),
		                          UnsafeNumericCast<ST>(scan_state.current_frame_of_reference), count);
	});
}

template <class T>
static void DecodeFORGroupDirect(BitpackingScanState<T> &scan_state, idx_t group_offset, idx_t offset_in_group,
                                 idx_t count, data_ptr_t result_buf, idx_t narrow_offset, PhysicalType stored_type) {
	constexpr idx_t GROUP = BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
	data_ptr_t pos = scan_state.current_group_ptr + group_offset * scan_state.current_width / 8;
	data_ptr_t src = pos - offset_in_group * scan_state.current_width / 8;
	auto width = scan_state.current_width;
	auto elem_size = GetTypeIdSize(stored_type);
	data_ptr_t dst = result_buf + narrow_offset * elem_size;
	FOR_SWITCH_STORED(stored_type, ST, {
		auto out = dst;
		uint8_t temp[GROUP * sizeof(uint64_t)];
		if (count != GROUP || offset_in_group != 0) {
			out = temp;
		}
		BitpackingPrimitives::UnPackBlock<ST>(out, src, width, true);
		if (out != dst) {
			memcpy(dst, temp + offset_in_group * elem_size, count * elem_size);
		}
		ApplyFrameOfReference<ST>(reinterpret_cast<ST *>(dst),
		                          UnsafeNumericCast<ST>(scan_state.current_frame_of_reference), count);
	});
}

template <class T>
static void AbortFOR(data_ptr_t result_buf, idx_t scanned, PhysicalType &for_st, bool &try_for) {
	if (for_st != PhysicalType::INVALID) {
		FORToFlat<T>(result_buf, scanned, for_st);
	}
	for_st = PhysicalType::INVALID;
	try_for = false;
}

} // namespace duckdb
