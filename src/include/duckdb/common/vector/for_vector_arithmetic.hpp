//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/for_vector_arithmetic.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/operator/numeric_binary_operators.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/for_vector.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

#include <type_traits>
#include <utility>

namespace duckdb {

static inline idx_t FORStoredTypeSize(PhysicalType type) {
	switch (type) {
	case PhysicalType::UINT8:
		return sizeof(uint8_t);
	case PhysicalType::UINT16:
		return sizeof(uint16_t);
	case PhysicalType::UINT32:
		return sizeof(uint32_t);
	case PhysicalType::UINT64:
		return sizeof(uint64_t);
	default:
		throw InternalException("Unsupported stored type for FOR vector size: %s", TypeIdToString(type));
	}
}

//===--------------------------------------------------------------------===//
// FOR arithmetic: operates on narrow stored data, producing narrow FOR result
//===--------------------------------------------------------------------===//
template <class OP>
struct FORBoundsSelector;
template <>
struct FORBoundsSelector<AddOperator> {
	static constexpr bool DIRECT_DELTA = true;
	static constexpr bool DIRECT_DELTA_USES_RIGHT_RANGE = false;
	template <class T>
	static bool Operation(T l, T r, T &out) {
		return TryAddOperator::Operation(l, r, out);
	}
	template <class T>
	static bool Bounds(T lmin, T lmax, T rmin, T rmax, T &result_min, T &result_max) {
		return Operation(lmin, rmin, result_min) && Operation(lmax, rmax, result_max);
	}
	template <class RESULT_T, class LEFT_T, class RIGHT_T>
	static inline RESULT_T DirectDelta(LEFT_T left_delta, RIGHT_T right_delta, uint64_t right_range_delta) {
		(void)right_range_delta;
		return UnsafeNumericCast<RESULT_T>(UnsafeNumericCast<uint64_t>(left_delta) +
		                                   UnsafeNumericCast<uint64_t>(right_delta));
	}
};
template <>
struct FORBoundsSelector<AddOperatorOverflowCheck> : FORBoundsSelector<AddOperator> {};
template <>
struct FORBoundsSelector<DecimalAddOverflowCheck> {
	static constexpr bool DIRECT_DELTA = true;
	static constexpr bool DIRECT_DELTA_USES_RIGHT_RANGE = false;
	template <class T>
	static bool Operation(T l, T r, T &out) {
		return TryDecimalAdd::Operation(l, r, out);
	}
	template <class T>
	static bool Bounds(T lmin, T lmax, T rmin, T rmax, T &result_min, T &result_max) {
		return Operation(lmin, rmin, result_min) && Operation(lmax, rmax, result_max);
	}
	template <class RESULT_T, class LEFT_T, class RIGHT_T>
	static inline RESULT_T DirectDelta(LEFT_T left_delta, RIGHT_T right_delta, uint64_t right_range_delta) {
		(void)right_range_delta;
		return UnsafeNumericCast<RESULT_T>(UnsafeNumericCast<uint64_t>(left_delta) +
		                                   UnsafeNumericCast<uint64_t>(right_delta));
	}
};
template <>
struct FORBoundsSelector<SubtractOperator> {
	static constexpr bool DIRECT_DELTA = true;
	static constexpr bool DIRECT_DELTA_USES_RIGHT_RANGE = false;
	template <class T>
	static bool Operation(T l, T r, T &out) {
		return TrySubtractOperator::Operation(l, r, out);
	}
	template <class T>
	static bool Bounds(T lmin, T lmax, T rmin, T rmax, T &result_min, T &result_max) {
		return Operation(lmin, rmax, result_min) && Operation(lmax, rmin, result_max);
	}
	template <class RESULT_T, class LEFT_T, class RIGHT_T>
	static inline RESULT_T DirectDelta(LEFT_T left_delta, RIGHT_T right_delta, uint64_t right_range_delta) {
		(void)right_range_delta;
		return UnsafeNumericCast<RESULT_T>(UnsafeNumericCast<uint64_t>(left_delta) -
		                                   UnsafeNumericCast<uint64_t>(right_delta));
	}
};
template <>
struct FORBoundsSelector<SubtractOperatorOverflowCheck> : FORBoundsSelector<SubtractOperator> {};
template <>
struct FORBoundsSelector<DecimalSubtractOverflowCheck> {
	static constexpr bool DIRECT_DELTA = true;
	static constexpr bool DIRECT_DELTA_USES_RIGHT_RANGE = false;
	template <class T>
	static bool Operation(T l, T r, T &out) {
		return TryDecimalSubtract::Operation(l, r, out);
	}
	template <class T>
	static bool Bounds(T lmin, T lmax, T rmin, T rmax, T &result_min, T &result_max) {
		return Operation(lmin, rmax, result_min) && Operation(lmax, rmin, result_max);
	}
	template <class RESULT_T, class LEFT_T, class RIGHT_T>
	static inline RESULT_T DirectDelta(LEFT_T left_delta, RIGHT_T right_delta, uint64_t right_range_delta) {
		(void)right_range_delta;
		return UnsafeNumericCast<RESULT_T>(UnsafeNumericCast<uint64_t>(left_delta) -
		                                   UnsafeNumericCast<uint64_t>(right_delta));
	}
};
template <>
struct FORBoundsSelector<MultiplyOperator> {
	static constexpr bool DIRECT_DELTA = false;
	static constexpr bool DIRECT_DELTA_USES_RIGHT_RANGE = false;
	template <class T>
	static bool Operation(T l, T r, T &out) {
		return TryMultiplyOperator::Operation(l, r, out);
	}
	template <class T>
	static bool Bounds(T lmin, T lmax, T rmin, T rmax, T &result_min, T &result_max) {
		T values[4];
		if (!Operation(lmin, rmin, values[0]) || !Operation(lmin, rmax, values[1]) ||
		    !Operation(lmax, rmin, values[2]) || !Operation(lmax, rmax, values[3])) {
			return false;
		}
		result_min = values[0];
		result_max = values[0];
		for (idx_t i = 1; i < 4; i++) {
			result_min = MinValue(result_min, values[i]);
			result_max = MaxValue(result_max, values[i]);
		}
		return true;
	}
};
template <>
struct FORBoundsSelector<MultiplyOperatorOverflowCheck> : FORBoundsSelector<MultiplyOperator> {};
template <>
struct FORBoundsSelector<DecimalMultiplyOverflowCheck> {
	static constexpr bool DIRECT_DELTA = false;
	static constexpr bool DIRECT_DELTA_USES_RIGHT_RANGE = false;
	template <class T>
	static bool Operation(T l, T r, T &out) {
		return TryDecimalMultiply::Operation(l, r, out);
	}
	template <class T>
	static bool Bounds(T lmin, T lmax, T rmin, T rmax, T &result_min, T &result_max) {
		T values[4];
		if (!Operation(lmin, rmin, values[0]) || !Operation(lmin, rmax, values[1]) ||
		    !Operation(lmax, rmin, values[2]) || !Operation(lmax, rmax, values[3])) {
			return false;
		}
		result_min = values[0];
		result_max = values[0];
		for (idx_t i = 1; i < 4; i++) {
			result_min = MinValue(result_min, values[i]);
			result_max = MaxValue(result_max, values[i]);
		}
		return true;
	}
};

template <class OP>
struct FORExecutionOperator {
	using type = OP;
};
template <>
struct FORExecutionOperator<AddOperatorOverflowCheck> {
	using type = AddOperator;
};
template <>
struct FORExecutionOperator<DecimalAddOverflowCheck> {
	using type = AddOperator;
};
template <>
struct FORExecutionOperator<SubtractOperatorOverflowCheck> {
	using type = SubtractOperator;
};
template <>
struct FORExecutionOperator<DecimalSubtractOverflowCheck> {
	using type = SubtractOperator;
};
template <>
struct FORExecutionOperator<MultiplyOperatorOverflowCheck> {
	using type = MultiplyOperator;
};
template <>
struct FORExecutionOperator<DecimalMultiplyOverflowCheck> {
	using type = MultiplyOperator;
};

template <class OP>
struct FORAdditiveConstantOp {
	static constexpr bool IS_SUPPORTED = false;
	static constexpr bool IS_ADD = false;
	static constexpr bool IS_SUBTRACT = false;
};
template <>
struct FORAdditiveConstantOp<AddOperator> {
	static constexpr bool IS_SUPPORTED = true;
	static constexpr bool IS_ADD = true;
	static constexpr bool IS_SUBTRACT = false;
};
template <>
struct FORAdditiveConstantOp<AddOperatorOverflowCheck> : FORAdditiveConstantOp<AddOperator> {};
template <>
struct FORAdditiveConstantOp<DecimalAddOverflowCheck> : FORAdditiveConstantOp<AddOperator> {};
template <>
struct FORAdditiveConstantOp<SubtractOperator> {
	static constexpr bool IS_SUPPORTED = true;
	static constexpr bool IS_ADD = false;
	static constexpr bool IS_SUBTRACT = true;
};
template <>
struct FORAdditiveConstantOp<SubtractOperatorOverflowCheck> : FORAdditiveConstantOp<SubtractOperator> {};
template <>
struct FORAdditiveConstantOp<DecimalSubtractOverflowCheck> : FORAdditiveConstantOp<SubtractOperator> {};

template <class DOMAIN_T, class OP>
static bool TryFORConstantAddSub(Vector &left, Vector &right, Vector &result, idx_t count) {
	using TRAITS = FORAdditiveConstantOp<OP>;
	if constexpr (!TRAITS::IS_SUPPORTED || std::is_same<DOMAIN_T, hugeint_t>::value ||
	              std::is_same<DOMAIN_T, uhugeint_t>::value) {
		return false;
	} else {
		FORVector::ScanData<DOMAIN_T> left_scan;
		FORVector::ScanData<DOMAIN_T> right_scan;
		const bool left_for = FORVector::TryGetScanData(left, left_scan);
		const bool right_for = FORVector::TryGetScanData(right, right_scan);
		if (left_for == right_for) {
			return false;
		}
		auto &scan = left_for ? left_scan : right_scan;
		auto &constant_vector = left_for ? right : left;
		if (constant_vector.GetVectorType() != VectorType::CONSTANT_VECTOR || ConstantVector::IsNull(constant_vector)) {
			return false;
		}
		auto constant = ConstantVector::GetData<DOMAIN_T>(constant_vector)[0];
		if (NumericLimits<DOMAIN_T>::IsSigned() && constant < DOMAIN_T(0)) {
			return false;
		}

		DOMAIN_T result_min;
		DOMAIN_T result_max;
		if constexpr (TRAITS::IS_ADD) {
			if (!FORBoundsSelector<OP>::template Bounds<DOMAIN_T>(
			        left_for ? DOMAIN_T(0) : constant, left_for ? scan.max_value : constant,
			        left_for ? constant : DOMAIN_T(0), left_for ? constant : scan.max_value, result_min, result_max)) {
				return false;
			}
		} else {
			if (left_for) {
				if (!FORBoundsSelector<OP>::template Bounds<DOMAIN_T>(DOMAIN_T(0), scan.max_value, constant, constant,
				                                                      result_min, result_max)) {
					return false;
				}
			} else {
				if (!FORBoundsSelector<OP>::template Bounds<DOMAIN_T>(constant, constant, DOMAIN_T(0), scan.max_value,
				                                                      result_min, result_max)) {
					return false;
				}
			}
		}
		if (result_min < DOMAIN_T(0)) {
			return false;
		}
		PhysicalType result_stored;
		if (!FORVector::TryGetStoredTypeForMax<DOMAIN_T>(result_max, result_stored)) {
			return false;
		}
		auto fill_result = [&](Vector &target, idx_t target_count) {
			FORVector::Create<DOMAIN_T>(target, result_stored, result_max);
			if (scan.validity->CanHaveNull()) {
				FORVector::Validity(target).Initialize(*scan.validity);
			}
			auto source =
			    FORVector::CreatePayloadView(scan.stored_type, FORVector::GetData(*scan.for_vec), target_count);
			auto target_payload = FORVector::CreatePayloadView(result_stored, FORVector::GetData(target), target_count);
			FOR_SWITCH_STORED(scan.stored_type, ST, {
				FOR_SWITCH_STORED(result_stored, RST, {
					Vector constant_view(Value::CreateValue(UnsafeNumericCast<RST>(constant)), count_t(target_count));
					if constexpr (TRAITS::IS_ADD) {
						BinaryExecutor::ExecuteStandard<ST, RST, RST, AddOperator>(source, constant_view,
						                                                           target_payload, target_count);
					} else if (left_for) {
						BinaryExecutor::ExecuteStandard<ST, RST, RST, SubtractOperator>(source, constant_view,
						                                                                target_payload, target_count);
					} else {
						BinaryExecutor::ExecuteStandard<RST, ST, RST, SubtractOperator>(constant_view, source,
						                                                                target_payload, target_count);
					}
				});
			});
		};
		if (scan.sel) {
			Vector child(result.GetType(), scan.for_vec->size());
			fill_result(child, scan.for_vec->size());
			auto entry = make_buffer<DictionaryEntry>(std::move(child));
			result.Dictionary(std::move(entry), *scan.sel, count);
		} else {
			fill_result(result, count);
		}
		return true;
	}
}

template <class LOGICAL_T, class OP, class BOUNDS>
static inline bool TryFORArithmetic(Vector &left, Vector &right, Vector &result, idx_t count) {
	struct Operand {
		bool is_for = false;
		LOGICAL_T constant_value {};
		LOGICAL_T min_value {};
		LOGICAL_T max_value {};
		FORVector::ScanData<LOGICAL_T> scan_data;
	};
	auto get_operand = [](Vector &vector, Operand &op) {
		op.is_for = FORVector::TryGetScanData(vector, op.scan_data);
		if (op.is_for) {
			op.min_value = LOGICAL_T(0);
			op.max_value = op.scan_data.max_value;
			return true;
		}
		if (vector.GetVectorType() != VectorType::CONSTANT_VECTOR || ConstantVector::IsNull(vector)) {
			return false;
		}
		auto val = ConstantVector::GetDataUnsafe<LOGICAL_T>(vector)[0];
		if (NumericLimits<LOGICAL_T>::IsSigned() && val < LOGICAL_T(0)) {
			return false;
		}
		op.constant_value = static_cast<LOGICAL_T>(val);
		op.min_value = op.constant_value;
		op.max_value = op.constant_value;
		return true;
	};

	Operand lop, rop;
	if (!get_operand(left, lop) || !get_operand(right, rop) || (!lop.is_for && !rop.is_for)) {
		return false;
	}
	unsafe_unique_array<data_t> lbuf;
	unsafe_unique_array<data_t> rbuf;
	const bool all_valid = (!lop.is_for || !lop.scan_data.validity->CanHaveNull()) &&
	                       (!rop.is_for || !rop.scan_data.validity->CanHaveNull());

	LOGICAL_T result_min {};
	LOGICAL_T result_max {};
	if (!BOUNDS::template Bounds<LOGICAL_T>(lop.min_value, lop.max_value, rop.min_value, rop.max_value, result_min,
	                                        result_max)) {
		return false;
	}
	PhysicalType rst;
	if (result_min < LOGICAL_T(0) || !FORVector::TryGetStoredTypeForMax<LOGICAL_T>(result_max, rst)) {
		return false;
	}
	auto write_result = [&](auto *rdata, idx_t i, LOGICAL_T value) {
		rdata[i] = UnsafeNumericCast<typename std::remove_pointer<decltype(rdata)>::type>(value);
	};
	auto try_direct_delta_fill = [&](data_ptr_t payload, idx_t target_count, bool raw_child) {
		if constexpr (!BOUNDS::DIRECT_DELTA) {
			return false;
		} else {
			if (!all_valid || !lop.is_for || !rop.is_for) {
				return false;
			}
			if (!raw_child && (lop.scan_data.sel || rop.scan_data.sel)) {
				return false;
			}
			FOR_SWITCH_STORED(rst, RS, {
				RS *__restrict rdata = reinterpret_cast<RS *>(payload);
				FOR_SWITCH_STORED(lop.scan_data.stored_type, LS, {
					const LS *__restrict ldata = reinterpret_cast<const LS *>(lop.scan_data.data);
					FOR_SWITCH_STORED(rop.scan_data.stored_type, RST, {
						const RST *__restrict rsrc = reinterpret_cast<const RST *>(rop.scan_data.data);
						for (idx_t i = 0; i < target_count; i++) {
							rdata[i] = BOUNDS::template DirectDelta<RS, LS, RST>(ldata[i], rsrc[i], 0);
						}
						return true;
					});
				});
			});
		}
	};
	auto try_multiply_delta_fill = [&](data_ptr_t payload, idx_t target_count, bool raw_child) {
		if constexpr (!std::is_same<OP, MultiplyOperator>::value || !std::is_integral<LOGICAL_T>::value) {
			return false;
		} else {
			if (!lop.is_for || !rop.is_for) {
				return false;
			}
			auto stored_fits_u32 = [](PhysicalType type) {
				return type == PhysicalType::UINT8 || type == PhysicalType::UINT16 || type == PhysicalType::UINT32;
			};
			const bool result_supported = rst == PhysicalType::UINT8 || rst == PhysicalType::UINT16 ||
			                              rst == PhysicalType::UINT32 || rst == PhysicalType::UINT64;
			if (!result_supported || !stored_fits_u32(lop.scan_data.stored_type) ||
			    !stored_fits_u32(rop.scan_data.stored_type)) {
				return false;
			}
			auto fits_u32 = [](LOGICAL_T value) {
				if constexpr (NumericLimits<LOGICAL_T>::IsSigned()) {
					if (value < LOGICAL_T(0)) {
						return false;
					}
				}
				return value <= static_cast<LOGICAL_T>(NumericLimits<uint32_t>::Maximum());
			};
			if (!fits_u32(result_max)) {
				return false;
			}

			FOR_SWITCH_STORED(rst, RS, {
				RS *__restrict rdata = reinterpret_cast<RS *>(payload);
				FOR_SWITCH_STORED(lop.scan_data.stored_type, LS, {
					const LS *__restrict ldata = reinterpret_cast<const LS *>(lop.scan_data.data);
					FOR_SWITCH_STORED(rop.scan_data.stored_type, RST, {
						const RST *__restrict rsrc = reinterpret_cast<const RST *>(rop.scan_data.data);
						if (raw_child || (!lop.scan_data.sel && !rop.scan_data.sel)) {
							for (idx_t i = 0; i < target_count; i++) {
								const auto ld = static_cast<uint32_t>(ldata[i]);
								const auto rd = static_cast<uint32_t>(rsrc[i]);
								if constexpr (sizeof(RS) <= sizeof(uint32_t)) {
									rdata[i] = UnsafeNumericCast<RS>(ld * rd);
								} else {
									rdata[i] = UnsafeNumericCast<RS>(static_cast<uint64_t>(ld) * rd);
								}
							}
						} else {
							for (idx_t i = 0; i < target_count; i++) {
								const auto lidx = lop.scan_data.sel ? lop.scan_data.sel->get_index(i) : i;
								const auto ridx = rop.scan_data.sel ? rop.scan_data.sel->get_index(i) : i;
								const auto ld = static_cast<uint32_t>(ldata[lidx]);
								const auto rd = static_cast<uint32_t>(rsrc[ridx]);
								if constexpr (sizeof(RS) <= sizeof(uint32_t)) {
									rdata[i] = UnsafeNumericCast<RS>(ld * rd);
								} else {
									rdata[i] = UnsafeNumericCast<RS>(static_cast<uint64_t>(ld) * rd);
								}
							}
						}
						return true;
					});
				});
			});
		}
	};
	auto generic_fill = [&](data_ptr_t payload, idx_t target_count, bool raw_child) {
		FOR_SWITCH_STORED(rst, RS, {
			RS *__restrict rdata = reinterpret_cast<RS *>(payload);
			FOR_SWITCH_STORED(lop.is_for ? lop.scan_data.stored_type : PhysicalType::UINT8, LS, {
				const LS *__restrict ldata =
				    lop.is_for ? (raw_child ? reinterpret_cast<const LS *>(lop.scan_data.data)
				                            : FORVector::CompactData<LS>(lop.scan_data, target_count, lbuf))
				               : static_cast<const LS *>(nullptr);
				FOR_SWITCH_STORED(rop.is_for ? rop.scan_data.stored_type : PhysicalType::UINT8, RST, {
					const RST *__restrict rsrc =
					    rop.is_for ? (raw_child ? reinterpret_cast<const RST *>(rop.scan_data.data)
					                            : FORVector::CompactData<RST>(rop.scan_data, target_count, rbuf))
					               : static_cast<const RST *>(nullptr);
					if (lop.is_for && rop.is_for) {
						for (idx_t i = 0; i < target_count; i++) {
							auto lhs = FORVector::WidenStored<LOGICAL_T>(ldata[i]);
							auto rhs = FORVector::WidenStored<LOGICAL_T>(rsrc[i]);
							auto value = OP::template Operation<LOGICAL_T, LOGICAL_T, LOGICAL_T>(lhs, rhs);
							write_result(rdata, i, value);
						}
					} else if (lop.is_for) {
						const auto rhs = rop.constant_value;
						for (idx_t i = 0; i < target_count; i++) {
							auto lhs = FORVector::WidenStored<LOGICAL_T>(ldata[i]);
							auto value = OP::template Operation<LOGICAL_T, LOGICAL_T, LOGICAL_T>(lhs, rhs);
							write_result(rdata, i, value);
						}
					} else {
						const auto lhs = lop.constant_value;
						for (idx_t i = 0; i < target_count; i++) {
							auto rhs = FORVector::WidenStored<LOGICAL_T>(rsrc[i]);
							auto value = OP::template Operation<LOGICAL_T, LOGICAL_T, LOGICAL_T>(lhs, rhs);
							write_result(rdata, i, value);
						}
					}
				});
			});
		});
	};

	const SelectionVector *result_sel = nullptr;
	idx_t child_count = count;
	auto check_operand_selection = [&](const Operand &op) {
		if (!op.is_for) {
			return true;
		}
		if (op.scan_data.sel) {
			if (!result_sel) {
				result_sel = op.scan_data.sel;
				child_count = op.scan_data.for_vec->size();
				return true;
			}
			return result_sel->data() == op.scan_data.sel->data() && child_count == op.scan_data.for_vec->size();
		}
		return !result_sel || child_count == op.scan_data.for_vec->size();
	};
	const auto raw_child_threshold = idx_t(128) * FORStoredTypeSize(rst);
	if (check_operand_selection(lop) && check_operand_selection(rop) && result_sel && count >= raw_child_threshold) {
		Vector child(result.GetType(), child_count);
		FORVector::Create<LOGICAL_T>(child, rst, result_max);
		if (!all_valid) {
			auto &result_validity = FORVector::Validity(child);
			auto apply_child_validity = [&](const Operand &op) {
				if (!op.is_for || !op.scan_data.validity->CanHaveNull()) {
					return;
				}
				for (idx_t i = 0; i < child_count; i++) {
					if (!op.scan_data.validity->RowIsValid(i)) {
						result_validity.SetInvalid(i);
					}
				}
			};
			apply_child_validity(lop);
			apply_child_validity(rop);
		}
		auto payload = FORVector::GetData(child);
		if (!try_direct_delta_fill(payload, child_count, true) &&
		    !try_multiply_delta_fill(payload, child_count, true)) {
			generic_fill(payload, child_count, true);
		}
		auto entry = make_buffer<DictionaryEntry>(std::move(child));
		result.Dictionary(std::move(entry), *result_sel, count);
		return true;
	}

	FORVector::Create<LOGICAL_T>(result, rst, result_max);
	if (!all_valid) {
		FORVector::CopyResultValidity(result, lop.is_for ? &lop.scan_data : nullptr,
		                              rop.is_for ? &rop.scan_data : nullptr, count);
	}
	auto payload = FORVector::GetData(result);
	if (!try_direct_delta_fill(payload, count, false) && !try_multiply_delta_fill(payload, count, false)) {
		generic_fill(payload, count, false);
	}
	return true;
}

} // namespace duckdb
