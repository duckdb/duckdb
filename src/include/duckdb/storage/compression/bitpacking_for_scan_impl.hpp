//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/bitpacking_for_scan_impl.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/compression/bitpacking_for_scan.hpp"
#include "duckdb/common/operator/multiply.hpp"

namespace duckdb {

template <class T>
static void DecodeFORBlock(BitpackingScanState<T> &scan_state, idx_t group_offset, idx_t offset_in_compression_group,
                           idx_t to_scan, T *result_ptr, bool apply_frame) {
	bool skip_sign_extend = true;

	data_ptr_t current_position_ptr = scan_state.current_group_ptr + group_offset * scan_state.current_width / 8;
	data_ptr_t decompression_group_start_pointer =
	    current_position_ptr - offset_in_compression_group * scan_state.current_width / 8;

	if (to_scan == BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE && offset_in_compression_group == 0) {
		BitpackingPrimitives::UnPackBlock<T>(data_ptr_cast(result_ptr), decompression_group_start_pointer,
		                                     scan_state.current_width, skip_sign_extend);
	} else {
		BitpackingPrimitives::UnPackBlock<T>(data_ptr_cast(scan_state.decompression_buffer),
		                                     decompression_group_start_pointer, scan_state.current_width,
		                                     skip_sign_extend);
		memcpy(result_ptr, scan_state.decompression_buffer + offset_in_compression_group, to_scan * sizeof(T));
	}
	if (apply_frame) {
		ApplyFrameOfReference<T>(result_ptr, scan_state.current_frame_of_reference, to_scan);
	}
}

template <class T, class T_S = typename MakeSigned<T>::type, class T_U = typename MakeUnsigned<T>::type>
void BitpackingScanPartialInternal(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                                   idx_t result_offset, bool allow_for) {
	auto &scan_state = state.scan_state->Cast<BitpackingScanState<T>>();

	auto result_buf = result.BufferMutable().GetData();
	T *result_data = reinterpret_cast<T *>(result_buf);
	result.SetVectorType(VectorType::FLAT_VECTOR);

	bool try_for = allow_for && sizeof(T) > 1 && result_offset == 0;
	PhysicalType for_st = PhysicalType::INVALID;
	T for_max = 0;
	idx_t scanned = 0;

	while (scanned < scan_count) {
		D_ASSERT(scan_state.current_group_offset <= BITPACKING_METADATA_GROUP_SIZE);
		if (scan_state.current_group_offset == BITPACKING_METADATA_GROUP_SIZE) {
			scan_state.LoadNextGroup();
		}
		idx_t offset_in_compression_group =
		    scan_state.current_group_offset % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

		if (scan_state.current_group.mode == BitpackingMode::CONSTANT) {
			idx_t remaining = scan_count - scanned;
			idx_t to_scan = MinValue(remaining, BITPACKING_METADATA_GROUP_SIZE - scan_state.current_group_offset);
			if (try_for) {
				PhysicalType requested_type;
				if (TryFORStoredTypeForMax<T>(scan_state.current_constant, requested_type)) {
					ReconcileFORType(result_buf, scanned, requested_type, for_st);
					for_max = MaxValue(for_max, scan_state.current_constant);
					FillFORConstant<T>(result_buf, scanned, to_scan, for_st, scan_state.current_constant);
					scanned += to_scan;
					scan_state.current_group_offset += to_scan;
					continue;
				}
				AbortFOR<T>(result_buf, scanned, for_st, try_for);
			}
			T *begin = result_data + result_offset + scanned;
			std::fill(begin, begin + to_scan, scan_state.current_constant);
			scanned += to_scan;
			scan_state.current_group_offset += to_scan;
			continue;
		}
		if (scan_state.current_group.mode == BitpackingMode::CONSTANT_DELTA) {
			idx_t remaining = scan_count - scanned;
			idx_t to_scan = MinValue(remaining, BITPACKING_METADATA_GROUP_SIZE - scan_state.current_group_offset);
			if (try_for) {
				T first_val = 0;
				T last_val = 0;
				T m_first;
				T m_last;
				T p_first;
				T p_last;
				const idx_t last_off = scan_state.current_group_offset + (to_scan - 1);
				bool can_for = TryCast::Operation<idx_t, T>(scan_state.current_group_offset, m_first) &&
				               TryCast::Operation<idx_t, T>(last_off, m_last) &&
				               TryMultiplyOperator::Operation(m_first, scan_state.current_constant, p_first) &&
				               TryAddOperator::Operation(p_first, scan_state.current_frame_of_reference, first_val) &&
				               TryMultiplyOperator::Operation(m_last, scan_state.current_constant, p_last) &&
				               TryAddOperator::Operation(p_last, scan_state.current_frame_of_reference, last_val);
				T group_min = MinValue(first_val, last_val);
				T group_max = MaxValue(first_val, last_val);
				PhysicalType requested_type;
				if (can_for && !(NumericLimits<T>::IsSigned() && group_min < T(0)) &&
				    TryFORStoredTypeForMax<T>(group_max, requested_type)) {
					ReconcileFORType(result_buf, scanned, requested_type, for_st);
					for_max = MaxValue(for_max, group_max);
					const T_U frame_u = static_cast<T_U>(scan_state.current_frame_of_reference);
					const T_U const_u = static_cast<T_U>(scan_state.current_constant);
					FOR_SWITCH_STORED(for_st, ST, {
						auto target = reinterpret_cast<ST *>(result_buf + scanned * GetTypeIdSize(for_st));
						for (idx_t i = 0; i < to_scan; i++) {
							const T_U multiplier = static_cast<T_U>(scan_state.current_group_offset + i);
							target[i] = static_cast<ST>(frame_u + const_u * multiplier);
						}
					});
					scanned += to_scan;
					scan_state.current_group_offset += to_scan;
					continue;
				}
				AbortFOR<T>(result_buf, scanned, for_st, try_for);
			}
			T *target_ptr = result_data + result_offset + scanned;
			for (idx_t i = 0; i < to_scan; i++) {
				idx_t multiplier = scan_state.current_group_offset + i;
				target_ptr[i] = static_cast<T>((static_cast<T_U>(scan_state.current_constant) * multiplier) +
				                               static_cast<T_U>(scan_state.current_frame_of_reference));
			}
			scanned += to_scan;
			scan_state.current_group_offset += to_scan;
			continue;
		}
		D_ASSERT(scan_state.current_group.mode == BitpackingMode::FOR ||
		         scan_state.current_group.mode == BitpackingMode::DELTA_FOR);
		idx_t to_scan = MinValue<idx_t>(scan_count - scanned, BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE -
		                                                          offset_in_compression_group);

		if (scan_state.current_group.mode == BitpackingMode::DELTA_FOR && try_for) {
			bool can_for = sizeof(T) > 4 && scan_state.current_width < 32 &&
			               (for_st == PhysicalType::INVALID || for_st == PhysicalType::UINT32);
			if (can_for) {
				T_S base = static_cast<T_S>(scan_state.current_delta_offset);
				T_S frame = static_cast<T_S>(scan_state.current_frame_of_reference);
				T_S remaining_horizon =
				    static_cast<T_S>(BITPACKING_METADATA_GROUP_SIZE - scan_state.current_group_offset);
				T_S max_delta;
				T_S span;
				T_S upper;
				can_for = base >= 0 && frame >= 0 &&
				          TryAddOperator::Operation(
				              frame, static_cast<T_S>((uint64_t(1) << scan_state.current_width) - 1), max_delta) &&
				          TryMultiplyOperator::Operation(remaining_horizon, max_delta, span) &&
				          TryAddOperator::Operation(base, span, upper) &&
				          upper <= static_cast<T_S>(NumericLimits<uint32_t>::Maximum());
			}
			if (can_for) {
				for_st = PhysicalType::UINT32;
				DecodeFORGroupDirect(scan_state, scan_state.current_group_offset, offset_in_compression_group, to_scan,
				                     result_buf, scanned, PhysicalType::UINT32);
				auto data32 = reinterpret_cast<uint32_t *>(result_buf + scanned * sizeof(uint32_t));
				const uint32_t last =
				    DeltaDecode<uint32_t>(data32, static_cast<uint32_t>(scan_state.current_delta_offset), to_scan);
				scan_state.current_delta_offset = static_cast<T>(last);
				for_max = MaxValue(for_max, static_cast<T>(last));
				scanned += to_scan;
				scan_state.current_group_offset += to_scan;
				continue;
			}
		}

		if (scan_state.current_group.mode == BitpackingMode::DELTA_FOR || !try_for) {
			AbortFOR<T>(result_buf, scanned, for_st, try_for);
			T *current_result_ptr = result_data + result_offset + scanned;
			if (scan_state.current_group.mode == BitpackingMode::DELTA_FOR) {
				DecodeFORBlock(scan_state, scan_state.current_group_offset, offset_in_compression_group, to_scan,
				               current_result_ptr, true);
				DeltaDecode<T_S>(reinterpret_cast<T_S *>(current_result_ptr),
				                 static_cast<T_S>(scan_state.current_delta_offset), to_scan);
				scan_state.current_delta_offset = current_result_ptr[to_scan - 1];
			} else {
				idx_t remaining = MinValue<idx_t>(scan_count - scanned,
				                                  BITPACKING_METADATA_GROUP_SIZE - scan_state.current_group_offset);
				idx_t batch_count = offset_in_compression_group == 0
				                        ? remaining & ~(BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE - 1)
				                        : 0;
				if (batch_count > 0) {
					DecodeFORBlocksBatchFlat(scan_state, scan_state.current_group_offset, batch_count,
					                         current_result_ptr);
					scanned += batch_count;
					scan_state.current_group_offset += batch_count;
					continue;
				}
				DecodeFORBlock(scan_state, scan_state.current_group_offset, offset_in_compression_group, to_scan,
				               current_result_ptr, true);
			}
		} else {
			PhysicalType requested_type;
			T group_max;
			if (!TryFORGroupType<T>(scan_state.current_frame_of_reference, scan_state.current_width, requested_type,
			                        group_max)) {
				AbortFOR<T>(result_buf, scanned, for_st, try_for);
				DecodeFORBlock(scan_state, scan_state.current_group_offset, offset_in_compression_group, to_scan,
				               result_data + result_offset + scanned, true);
			} else {
				ReconcileFORType(result_buf, scanned, requested_type, for_st);
				for_max = MaxValue(for_max, group_max);
				DecodeFORGroupDirect(scan_state, scan_state.current_group_offset, offset_in_compression_group, to_scan,
				                     result_buf, scanned, for_st);
				scanned += to_scan;
				scan_state.current_group_offset += to_scan;
				idx_t remaining = MinValue<idx_t>(scan_count - scanned,
				                                  BITPACKING_METADATA_GROUP_SIZE - scan_state.current_group_offset);
				idx_t batch_count = remaining & ~(BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE - 1);
				if (batch_count > 0) {
					DecodeFORGroupsBatch(scan_state, scan_state.current_group_offset, batch_count, result_buf, scanned,
					                     for_st);
					scanned += batch_count;
					scan_state.current_group_offset += batch_count;
				}
				idx_t tail = remaining - batch_count;
				if (tail > 0) {
					DecodeFORGroupDirect(scan_state, scan_state.current_group_offset, 0, tail, result_buf, scanned,
					                     for_st);
					scanned += tail;
					scan_state.current_group_offset += tail;
				}
				continue;
			}
		}

		scanned += to_scan;
		scan_state.current_group_offset += to_scan;
	}

	if (sizeof(T) > 1 && for_st != PhysicalType::INVALID) {
		FORVector::Create<T>(result, for_st, for_max);
	}
}

template <class T, class T_S = typename MakeSigned<T>::type, class T_U = typename MakeUnsigned<T>::type>
void BitpackingScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                           idx_t result_offset) {
	BitpackingScanPartialInternal<T>(segment, state, scan_count, result, result_offset, false);
}

template <class T>
void BitpackingScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	BitpackingScanPartialInternal<T>(segment, state, scan_count, result, 0, true);
}

} // namespace duckdb
