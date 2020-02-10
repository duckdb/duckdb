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
	static inline RESULT_TYPE Operation(FUNC fun, INPUT_TYPE input) {
		return OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input);
	}
};

struct UnaryLambdaWrapper {
	template <class FUNC, class OP, class INPUT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, INPUT_TYPE input) {
		return fun(input);
	}
};

struct UnaryExecutor {
private:
	template <class INPUT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC, bool IGNORE_NULL>
	static inline void ExecuteLoop(INPUT_TYPE *__restrict ldata, RESULT_TYPE *__restrict result_data, index_t count,
	                               sel_t *__restrict sel_vector, nullmask_t nullmask, FUNC fun) {
		ASSERT_RESTRICT(ldata, ldata + count, result_data, result_data + count);

		if (IGNORE_NULL && nullmask.any()) {
			VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
				if (!nullmask[i]) {
					result_data[i] = OPWRAPPER::template Operation<FUNC, OP, INPUT_TYPE, RESULT_TYPE>(fun, ldata[i]);
				}
			});
		} else {
			VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
				result_data[i] = OPWRAPPER::template Operation<FUNC, OP, INPUT_TYPE, RESULT_TYPE>(fun, ldata[i]);
			});
		}
	}

	template <class INPUT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC, bool IGNORE_NULL>
	static inline void ExecuteStandard(Vector &input, Vector &result, FUNC fun) {
		auto result_data = (RESULT_TYPE *)result.GetData();

		if (input.vector_type == VectorType::CONSTANT_VECTOR) {
			auto ldata = (INPUT_TYPE *)input.GetData();

			result.vector_type = VectorType::CONSTANT_VECTOR;
			if (input.nullmask[0]) {
				result.nullmask[0] = true;
			} else {
				result.nullmask[0] = false;
				result_data[0] = OPWRAPPER::template Operation<FUNC, OP, INPUT_TYPE, RESULT_TYPE>(fun, ldata[0]);
			}
		} else {
			input.Normalify();
			auto ldata = (INPUT_TYPE *)input.GetData();

			result.vector_type = VectorType::FLAT_VECTOR;
			result.nullmask = input.nullmask;
			ExecuteLoop<INPUT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, IGNORE_NULL>(
			    ldata, result_data, input.size(), input.sel_vector(), input.nullmask, fun);
		}
		result.SetCount(input.size());
		result.SetSelVector(input.sel_vector());
	}

public:
	template <class INPUT_TYPE, class RESULT_TYPE, class OP, bool IGNORE_NULL = false>
	static void Execute(Vector &input, Vector &result) {
		ExecuteStandard<INPUT_TYPE, RESULT_TYPE, UnaryOperatorWrapper, OP, bool, IGNORE_NULL>(input, result, false);
	}

	template <class INPUT_TYPE, class RESULT_TYPE, bool IGNORE_NULL = false,
	          class FUNC = std::function<RESULT_TYPE(INPUT_TYPE)>>
	static void Execute(Vector &input, Vector &result, FUNC fun) {
		ExecuteStandard<INPUT_TYPE, RESULT_TYPE, UnaryLambdaWrapper, bool, FUNC, IGNORE_NULL>(input, result, fun);
	}
};

} // namespace duckdb
