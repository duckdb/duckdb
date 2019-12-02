//===--------------------------------------------------------------------===//
// numeric_inplace_operators.cpp
// Description: This file contains the implementation of numeric inplace ops
// += *= /= -= %=
//===--------------------------------------------------------------------===//

#include "duckdb/common/operator/numeric_inplace_operators.hpp"

#include "duckdb/common/types/constant_vector.hpp"
#include "duckdb/common/vector_operations/inplace_loops.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <algorithm>

using namespace duckdb;
using namespace std;

//===--------------------------------------------------------------------===//
// In-Place Addition
//===--------------------------------------------------------------------===//
// left += right
void VectorOperations::AddInPlace(Vector &result, Vector &input) {
	INPLACE_TYPE_CHECK(input, result);
	// the inplace loops take the result as the last parameter
	switch (input.type) {
	case TypeId::TINYINT:
		templated_inplace_loop<int8_t, int8_t, duckdb::AddInPlace>(input, result);
		break;
	case TypeId::SMALLINT:
		templated_inplace_loop<int16_t, int16_t, duckdb::AddInPlace>(input, result);
		break;
	case TypeId::INTEGER:
		templated_inplace_loop<int32_t, int32_t, duckdb::AddInPlace>(input, result);
		break;
	case TypeId::BIGINT:
		templated_inplace_loop<int64_t, int64_t, duckdb::AddInPlace>(input, result);
		break;
	case TypeId::HASH:
		templated_inplace_loop<uint64_t, uint64_t, duckdb::AddInPlace>(input, result);
		break;
	case TypeId::POINTER:
		templated_inplace_loop<uintptr_t, uintptr_t, duckdb::AddInPlace>(input, result);
		break;
	case TypeId::FLOAT:
		templated_inplace_loop<float, float, duckdb::AddInPlace>(input, result);
		break;
	case TypeId::DOUBLE:
		templated_inplace_loop<double, double, duckdb::AddInPlace>(input, result);
		break;
	default:
		throw InvalidTypeException(input.type, "Invalid type for addition");
	}
}

void VectorOperations::AddInPlace(Vector &left, int64_t right) {
	Value right_value = Value::Numeric(left.type, right);
	ConstantVector right_vector(right_value);
	VectorOperations::AddInPlace(left, right_vector);
}

template <class LEFT_TYPE, class RESULT_TYPE, class OP>
void templated_inplace_divmod_loop(Vector &input, Vector &result) {
	auto rdata = (LEFT_TYPE *)input.data;
	auto result_data = (RESULT_TYPE *)result.data;
	if (input.IsConstant()) { // a % 0 -> NULL
		if (input.nullmask[0] || input.GetValue(0) == Value::Numeric(input.type, 0)) {
			result.nullmask.set();
		} else {
			VectorOperations::Exec(result.sel_vector, result.count,
			                       [&](index_t i, index_t k) { OP::Operation(result_data[i], rdata[0]); });
		}
	} else {
		// OR nullmasks together
		result.nullmask = input.nullmask | result.nullmask;
		assert(result.sel_vector == input.sel_vector);
		ASSERT_RESTRICT(rdata, rdata + result.count, result_data, result_data + result.count);
		VectorOperations::Exec(result.sel_vector, result.count, [&](index_t i, index_t k) {
			if (rdata[i] == 0) {
				result.nullmask[i] = true;
			} else {
				OP::Operation(result_data[i], rdata[i]);
			}
		});
	}
}

//===--------------------------------------------------------------------===//
// In-Place Modulo
//===--------------------------------------------------------------------===//
// left %= right
void VectorOperations::ModuloInPlace(Vector &result, Vector &input) {
	INPLACE_TYPE_CHECK(input, result);
	// the in-place loops take the result as the last parameter
	switch (input.type) {
	case TypeId::TINYINT:
		templated_inplace_divmod_loop<int8_t, int8_t, duckdb::ModuloIntInPlace>(input, result);
		break;
	case TypeId::SMALLINT:
		templated_inplace_divmod_loop<int16_t, int16_t, duckdb::ModuloIntInPlace>(input, result);
		break;
	case TypeId::INTEGER:
		templated_inplace_divmod_loop<int32_t, int32_t, duckdb::ModuloIntInPlace>(input, result);
		break;
	case TypeId::BIGINT:
		templated_inplace_divmod_loop<int64_t, int64_t, duckdb::ModuloIntInPlace>(input, result);
		break;
	case TypeId::HASH:
		templated_inplace_divmod_loop<uint64_t, uint64_t, duckdb::ModuloIntInPlace>(input, result);
		break;
	case TypeId::FLOAT:
		templated_inplace_divmod_loop<float, float, duckdb::ModuloRealInPlace>(input, result);
		break;
	case TypeId::DOUBLE:
		templated_inplace_divmod_loop<double, double, duckdb::ModuloRealInPlace>(input, result);
		break;
	default:
		throw InvalidTypeException(input.type, "Invalid type for in-place modulo");
	}
}

void VectorOperations::ModuloInPlace(Vector &left, int64_t right) {
	Value right_value = Value::Numeric(left.type, right);
	ConstantVector right_vector(right_value);
	VectorOperations::ModuloInPlace(left, right_vector);
}
