//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/cast/vector_cast_helpers.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/common/vector_operations/decimal_cast.hpp"
#include "duckdb/common/operator/numeric_cast.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

namespace duckdb {

template <class OP>
struct VectorStringCastOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto result = (Vector *)dataptr;
		return OP::template Operation<INPUT_TYPE>(input, *result);
	}
};

struct VectorTryCastData {
	VectorTryCastData(Vector &result_p, string *error_message_p, bool strict_p)
	    : result(result_p), error_message(error_message_p), strict(strict_p) {
	}

	Vector &result;
	string *error_message;
	bool strict;
	bool all_converted = true;
};

template <class OP>
struct VectorTryCastOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		RESULT_TYPE output;
		if (DUCKDB_LIKELY(OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input, output))) {
			return output;
		}
		auto data = (VectorTryCastData *)dataptr;
		return HandleVectorCastError::Operation<RESULT_TYPE>(CastExceptionText<INPUT_TYPE, RESULT_TYPE>(input), mask,
		                                                     idx, data->error_message, data->all_converted);
	}
};

template <class OP>
struct VectorTryCastStrictOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto data = (VectorTryCastData *)dataptr;
		RESULT_TYPE output;
		if (DUCKDB_LIKELY(OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input, output, data->strict))) {
			return output;
		}
		return HandleVectorCastError::Operation<RESULT_TYPE>(CastExceptionText<INPUT_TYPE, RESULT_TYPE>(input), mask,
		                                                     idx, data->error_message, data->all_converted);
	}
};

template <class OP>
struct VectorTryCastErrorOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto data = (VectorTryCastData *)dataptr;
		RESULT_TYPE output;
		if (DUCKDB_LIKELY(
		        OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input, output, data->error_message, data->strict))) {
			return output;
		}
		bool has_error = data->error_message && !data->error_message->empty();
		return HandleVectorCastError::Operation<RESULT_TYPE>(
		    has_error ? *data->error_message : CastExceptionText<INPUT_TYPE, RESULT_TYPE>(input), mask, idx,
		    data->error_message, data->all_converted);
	}
};

template <class OP>
struct VectorTryCastStringOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto data = (VectorTryCastData *)dataptr;
		RESULT_TYPE output;
		if (DUCKDB_LIKELY(OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input, output, data->result,
		                                                                  data->error_message, data->strict))) {
			return output;
		}
		return HandleVectorCastError::Operation<RESULT_TYPE>(CastExceptionText<INPUT_TYPE, RESULT_TYPE>(input), mask,
		                                                     idx, data->error_message, data->all_converted);
	}
};

template <class SRC, class DST, class OP>
static bool TemplatedVectorCastLoop(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	UnaryExecutor::Execute<SRC, DST, OP>(source, result, count);
	return true;
}

template <class SRC, class DST, class OP>
static bool TemplatedVectorTryCastLoop(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	VectorTryCastData input(result, parameters.error_message, parameters.strict);
	UnaryExecutor::GenericExecute<SRC, DST, OP>(source, result, count, &input, parameters.error_message);
	return input.all_converted;
}

template <class SRC, class DST, class OP>
static bool VectorTryCastLoop(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	return TemplatedVectorTryCastLoop<SRC, DST, VectorTryCastOperator<OP>>(source, result, count, parameters);
}

template <class SRC, class DST, class OP>
static bool VectorTryCastStrictLoop(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	return TemplatedVectorTryCastLoop<SRC, DST, VectorTryCastStrictOperator<OP>>(source, result, count, parameters);
}

template <class SRC, class DST, class OP>
static bool VectorTryCastErrorLoop(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	return TemplatedVectorTryCastLoop<SRC, DST, VectorTryCastErrorOperator<OP>>(source, result, count, parameters);
}

template <class SRC, class DST, class OP>
static bool VectorTryCastStringLoop(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	return TemplatedVectorTryCastLoop<SRC, DST, VectorTryCastStringOperator<OP>>(source, result, count, parameters);
}

template <class SRC, class OP>
static bool VectorStringCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	D_ASSERT(result.GetType().InternalType() == PhysicalType::VARCHAR);
	UnaryExecutor::GenericExecute<SRC, string_t, VectorStringCastOperator<OP>>(source, result, count, (void *)&result);
	return true;
}

} // namespace duckdb
