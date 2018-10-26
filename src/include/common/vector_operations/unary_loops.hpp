//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// common/vector_operations/unary_loops.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#include "common/exception.hpp"
#include "common/types/vector.hpp"

namespace duckdb {

inline void UNARY_TYPE_CHECK(Vector &input, Vector &result) {
	if (input.type != result.type) {
		throw TypeMismatchException(input.type, result.type,
		                            "input and result types must be the same");
	}
}

template <class LEFT_TYPE, class RESULT_TYPE, class OP>
static inline void
unary_loop_function_array(LEFT_TYPE *__restrict ldata,
                          RESULT_TYPE *__restrict result_data, size_t count,
                          sel_t *__restrict sel_vector) {
	ASSERT_RESTRICT(ldata, ldata + count, result_data, result_data + count);
	if (sel_vector) {
		for (size_t i = 0; i < count; i++) {
			result_data[sel_vector[i]] = OP::Operation(ldata[sel_vector[i]]);
		}
	} else {
		for (size_t i = 0; i < count; i++) {
			result_data[i] = OP::Operation(ldata[i]);
		}
	}
}

template <class LEFT_TYPE, class RESULT_TYPE, class OP>
void templated_unary_loop(Vector &input, Vector &result) {
	auto ldata = (LEFT_TYPE *)input.data;
	auto result_data = (RESULT_TYPE *)result.data;

	unary_loop_function_array<LEFT_TYPE, RESULT_TYPE, OP>(
	    ldata, result_data, input.count, input.sel_vector);
	result.nullmask = input.nullmask;
	result.sel_vector = input.sel_vector;
	result.count = input.count;
}

} // namespace duckdb
