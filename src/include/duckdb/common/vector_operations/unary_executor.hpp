//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/unary_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <functional>

namespace duckdb {

struct UnaryOperatorWrapper {
	template <class FUNC, class OP, class INPUT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, INPUT_TYPE input, ValidityMask &mask, idx_t idx) {
		return OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input);
	}

	static bool AddsNulls() {
		return false;
	}
};

struct UnaryLambdaWrapper {
	template <class FUNC, class OP, class INPUT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, INPUT_TYPE input, ValidityMask &mask, idx_t idx) {
		return fun(input);
	}

	static bool AddsNulls() {
		return false;
	}
};

struct UnaryExecutor {
private:
	template <class INPUT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC>
	static inline void ExecuteLoop(INPUT_TYPE *__restrict ldata, RESULT_TYPE *__restrict result_data, idx_t count,
	                               const SelectionVector *__restrict sel_vector, ValidityMask &mask,
	                               ValidityMask &result_mask, FUNC fun) {
		ASSERT_RESTRICT(ldata, ldata + count, result_data, result_data + count);

		if (!mask.AllValid()) {
			result_mask.EnsureWritable();
			for (idx_t i = 0; i < count; i++) {
				auto idx = sel_vector->get_index(i);
				if (mask.RowIsValidUnsafe(idx)) {
					result_data[i] = OPWRAPPER::template Operation<FUNC, OP, INPUT_TYPE, RESULT_TYPE>(fun, ldata[idx],
					                                                                                  result_mask, i);
				} else {
					result_mask.SetInvalid(i);
				}
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				auto idx = sel_vector->get_index(i);
				result_data[i] =
				    OPWRAPPER::template Operation<FUNC, OP, INPUT_TYPE, RESULT_TYPE>(fun, ldata[idx], result_mask, i);
			}
		}
	}

	template <class INPUT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC>
	static inline void ExecuteFlat(INPUT_TYPE *__restrict ldata, RESULT_TYPE *__restrict result_data, idx_t count,
	                               ValidityMask &mask, ValidityMask &result_mask, FUNC fun) {
		ASSERT_RESTRICT(ldata, ldata + count, result_data, result_data + count);

		if (!mask.AllValid()) {
			if (!OPWRAPPER::AddsNulls()) {
				result_mask.Initialize(mask);
			} else {
				result_mask.Copy(mask, count);
			}
			idx_t base_idx = 0;
			auto entry_count = ValidityMask::EntryCount(count);
			for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
				auto validity_entry = mask.GetValidityEntry(entry_idx);
				idx_t next = MinValue<idx_t>(base_idx + ValidityMask::BITS_PER_VALUE, count);
				if (ValidityMask::AllValid(validity_entry)) {
					// all valid: perform operation
					for (; base_idx < next; base_idx++) {
						result_data[base_idx] = OPWRAPPER::template Operation<FUNC, OP, INPUT_TYPE, RESULT_TYPE>(
						    fun, ldata[base_idx], result_mask, base_idx);
					}
				} else if (ValidityMask::NoneValid(validity_entry)) {
					// nothing valid: skip all
					base_idx = next;
					continue;
				} else {
					// partially valid: need to check individual elements for validity
					idx_t start = base_idx;
					for (; base_idx < next; base_idx++) {
						if (ValidityMask::RowIsValid(validity_entry, base_idx - start)) {
							D_ASSERT(mask.RowIsValid(base_idx));
							result_data[base_idx] = OPWRAPPER::template Operation<FUNC, OP, INPUT_TYPE, RESULT_TYPE>(
							    fun, ldata[base_idx], result_mask, base_idx);
						}
					}
				}
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				result_data[i] =
				    OPWRAPPER::template Operation<FUNC, OP, INPUT_TYPE, RESULT_TYPE>(fun, ldata[i], result_mask, i);
			}
		}
	}

	template <class INPUT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC>
	static inline void ExecuteStandard(Vector &input, Vector &result, idx_t count, FUNC fun) {
		switch (input.GetVectorType()) {
		case VectorType::CONSTANT_VECTOR: {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			auto result_data = ConstantVector::GetData<RESULT_TYPE>(result);
			auto ldata = ConstantVector::GetData<INPUT_TYPE>(input);

			if (ConstantVector::IsNull(input)) {
				ConstantVector::SetNull(result, true);
			} else {
				ConstantVector::SetNull(result, false);
				*result_data = OPWRAPPER::template Operation<FUNC, OP, INPUT_TYPE, RESULT_TYPE>(
				    fun, *ldata, ConstantVector::Validity(result), 0);
			}
			break;
		}
		case VectorType::FLAT_VECTOR: {
			result.SetVectorType(VectorType::FLAT_VECTOR);
			auto result_data = FlatVector::GetData<RESULT_TYPE>(result);
			auto ldata = FlatVector::GetData<INPUT_TYPE>(input);

			ExecuteFlat<INPUT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC>(
			    ldata, result_data, count, FlatVector::Validity(input), FlatVector::Validity(result), fun);
			break;
		}
		default: {
			VectorData vdata;
			input.Orrify(count, vdata);

			result.SetVectorType(VectorType::FLAT_VECTOR);
			auto result_data = FlatVector::GetData<RESULT_TYPE>(result);
			auto ldata = (INPUT_TYPE *)vdata.data;

			ExecuteLoop<INPUT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC>(
			    ldata, result_data, count, vdata.sel, vdata.validity, FlatVector::Validity(result), fun);
			break;
		}
		}
	}

public:
	template <class INPUT_TYPE, class RESULT_TYPE, class OP, class OPWRAPPER = UnaryOperatorWrapper>
	static void Execute(Vector &input, Vector &result, idx_t count) {
		ExecuteStandard<INPUT_TYPE, RESULT_TYPE, OPWRAPPER, OP, bool>(input, result, count, false);
	}

	template <class INPUT_TYPE, class RESULT_TYPE, class OPWRAPPER=UnaryLambdaWrapper, class FUNC = std::function<RESULT_TYPE(INPUT_TYPE)>>
	static void Execute(Vector &input, Vector &result, idx_t count, FUNC fun) {
		ExecuteStandard<INPUT_TYPE, RESULT_TYPE, OPWRAPPER, bool, FUNC>(input, result, count, fun);
	}
};

} // namespace duckdb
