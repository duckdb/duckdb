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
	template <class INPUT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC, bool IGNORE_NULL, bool HAS_SEL_VECTOR>
	static inline void ExecuteLoop(INPUT_TYPE *__restrict ldata, RESULT_TYPE *__restrict result_data, idx_t count,
	                               SelectionVector *__restrict sel_vector, nullmask_t &nullmask, nullmask_t &result_nullmask, FUNC fun) {
		ASSERT_RESTRICT(ldata, ldata + count, result_data, result_data + count);

		if (IGNORE_NULL && nullmask.any()) {
			if (!HAS_SEL_VECTOR) {
				result_nullmask = nullmask;
			}
			for(idx_t i = 0; i < count; i++) {
				auto idx = HAS_SEL_VECTOR ? i : sel_vector->get_index(i);
				if (!nullmask[idx]) {
					result_data[i] = OPWRAPPER::template Operation<FUNC, OP, INPUT_TYPE, RESULT_TYPE>(fun, ldata[idx]);
				} else if (HAS_SEL_VECTOR) {
					result_nullmask[i] = true;
				}
			}
		} else {
			for(idx_t i = 0; i < count; i++) {
				auto idx = HAS_SEL_VECTOR ? i : sel_vector->get_index(i);
				result_data[i] = OPWRAPPER::template Operation<FUNC, OP, INPUT_TYPE, RESULT_TYPE>(fun, ldata[idx]);
			}
		}
	}

	template <class INPUT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC, bool IGNORE_NULL>
	static inline void ExecuteStandard(Vector &input, Vector &result, FUNC fun) {
		assert(input.SameCardinality(result));

		switch(input.vector_type) {
		case VectorType::CONSTANT_VECTOR:{
			result.vector_type = VectorType::CONSTANT_VECTOR;
			auto result_data = ConstantVector::GetData<RESULT_TYPE>(result);
			auto ldata = ConstantVector::GetData<INPUT_TYPE>(input);

			if (ConstantVector::IsNull(input)) {
				ConstantVector::SetNull(result, true);
			} else {
				ConstantVector::SetNull(result, false);
				result_data[0] = OPWRAPPER::template Operation<FUNC, OP, INPUT_TYPE, RESULT_TYPE>(fun, ldata[0]);
			}
			break;
		}
		case VectorType::DICTIONARY_VECTOR:  {
			auto &sel = DictionaryVector::SelectionVector(input);
			auto &child = DictionaryVector::Child(input);
			child.Normalify();

			result.vector_type = VectorType::FLAT_VECTOR;
			auto result_data = FlatVector::GetData<RESULT_TYPE>(result);
			auto ldata = FlatVector::GetData<INPUT_TYPE>(child);

			ExecuteLoop<INPUT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, IGNORE_NULL, true>(
			    ldata, result_data, input.size(), &sel, FlatVector::Nullmask(child), FlatVector::Nullmask(result), fun);
			break;
		}
		default: {
			input.Normalify();

			result.vector_type = VectorType::FLAT_VECTOR;
			auto result_data = FlatVector::GetData<RESULT_TYPE>(result);
			auto ldata = FlatVector::GetData<INPUT_TYPE>(input);

			ExecuteLoop<INPUT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, IGNORE_NULL, false>(
			    ldata, result_data, input.size(), nullptr, FlatVector::Nullmask(input), FlatVector::Nullmask(result), fun);
			break;
		}
		}
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
