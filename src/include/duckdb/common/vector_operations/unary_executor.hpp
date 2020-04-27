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
	static inline RESULT_TYPE Operation(FUNC fun, INPUT_TYPE input, nullmask_t &nullmask, idx_t idx) {
		return OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input);
	}
};

struct UnaryLambdaWrapper {
	template <class FUNC, class OP, class INPUT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, INPUT_TYPE input, nullmask_t &nullmask, idx_t idx) {
		return fun(input);
	}
};

struct UnaryExecutor {
private:
	template <class INPUT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC, bool IGNORE_NULL>
	static inline void ExecuteLoop(INPUT_TYPE *__restrict ldata, RESULT_TYPE *__restrict result_data, idx_t count,
	                               const SelectionVector *__restrict sel_vector, nullmask_t &nullmask,
	                               nullmask_t &result_nullmask, FUNC fun) {
		ASSERT_RESTRICT(ldata, ldata + count, result_data, result_data + count);

		if (nullmask.any()) {
			for (idx_t i = 0; i < count; i++) {
				auto idx = sel_vector->get_index(i);
				if (!nullmask[idx]) {
					result_data[i] = OPWRAPPER::template Operation<FUNC, OP, INPUT_TYPE, RESULT_TYPE>(
					    fun, ldata[idx], result_nullmask, i);
				} else {
					result_nullmask[i] = true;
				}
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				auto idx = sel_vector->get_index(i);
				result_data[i] = OPWRAPPER::template Operation<FUNC, OP, INPUT_TYPE, RESULT_TYPE>(fun, ldata[idx],
				                                                                                  result_nullmask, i);
			}
		}
	}

	template <class INPUT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC, bool IGNORE_NULL>
	static inline void ExecuteFlat(INPUT_TYPE *__restrict ldata, RESULT_TYPE *__restrict result_data, idx_t count,
	                               nullmask_t &nullmask, nullmask_t &result_nullmask, FUNC fun) {
		ASSERT_RESTRICT(ldata, ldata + count, result_data, result_data + count);

		if (IGNORE_NULL && nullmask.any()) {
			result_nullmask = nullmask;
			for (idx_t i = 0; i < count; i++) {
				if (!nullmask[i]) {
					result_data[i] = OPWRAPPER::template Operation<FUNC, OP, INPUT_TYPE, RESULT_TYPE>(
					    fun, ldata[i], result_nullmask, i);
				}
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				result_data[i] =
				    OPWRAPPER::template Operation<FUNC, OP, INPUT_TYPE, RESULT_TYPE>(fun, ldata[i], result_nullmask, i);
			}
		}
	}

	template <class INPUT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC, bool IGNORE_NULL>
	static inline void ExecuteStandard(Vector &input, Vector &result, idx_t count, FUNC fun) {
		switch (input.vector_type) {
		case VectorType::CONSTANT_VECTOR: {
			result.vector_type = VectorType::CONSTANT_VECTOR;
			auto result_data = ConstantVector::GetData<RESULT_TYPE>(result);
			auto ldata = ConstantVector::GetData<INPUT_TYPE>(input);

			if (ConstantVector::IsNull(input)) {
				ConstantVector::SetNull(result, true);
			} else {
				ConstantVector::SetNull(result, false);
				*result_data = OPWRAPPER::template Operation<FUNC, OP, INPUT_TYPE, RESULT_TYPE>(
				    fun, *ldata, ConstantVector::Nullmask(result), 0);
			}
			break;
		}
		case VectorType::FLAT_VECTOR: {
			result.vector_type = VectorType::FLAT_VECTOR;
			auto result_data = FlatVector::GetData<RESULT_TYPE>(result);
			auto ldata = FlatVector::GetData<INPUT_TYPE>(input);

			FlatVector::SetNullmask(result, FlatVector::Nullmask(input));

			ExecuteFlat<INPUT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, IGNORE_NULL>(
			    ldata, result_data, count, FlatVector::Nullmask(input), FlatVector::Nullmask(result), fun);
			break;
		}
		default: {
			VectorData vdata;
			input.Orrify(count, vdata);

			result.vector_type = VectorType::FLAT_VECTOR;
			auto result_data = FlatVector::GetData<RESULT_TYPE>(result);
			auto ldata = (INPUT_TYPE *)vdata.data;

			ExecuteLoop<INPUT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, IGNORE_NULL>(
			    ldata, result_data, count, vdata.sel, *vdata.nullmask, FlatVector::Nullmask(result), fun);
			break;
		}
		}
	}

public:
	template <class INPUT_TYPE, class RESULT_TYPE, class OP, bool IGNORE_NULL = false,
	          class OPWRAPPER = UnaryOperatorWrapper>
	static void Execute(Vector &input, Vector &result, idx_t count) {
		ExecuteStandard<INPUT_TYPE, RESULT_TYPE, OPWRAPPER, OP, bool, IGNORE_NULL>(input, result, count, false);
	}

	template <class INPUT_TYPE, class RESULT_TYPE, bool IGNORE_NULL = false,
	          class FUNC = std::function<RESULT_TYPE(INPUT_TYPE)>>
	static void Execute(Vector &input, Vector &result, idx_t count, FUNC fun) {
		ExecuteStandard<INPUT_TYPE, RESULT_TYPE, UnaryLambdaWrapper, bool, FUNC, IGNORE_NULL>(input, result, count,
		                                                                                      fun);
	}
};

} // namespace duckdb
