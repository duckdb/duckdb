//===--------------------------------------------------------------------===//
// boolean_operators.cpp
// Description: This file contains the implementation of the boolean
// operations AND OR !
//===--------------------------------------------------------------------===//

#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// AND/OR
//===--------------------------------------------------------------------===//
template <class OP>
static void TemplatedBooleanNullmask(Vector &left, Vector &right, Vector &result, idx_t count) {
	D_ASSERT(left.GetType().id() == LogicalTypeId::BOOLEAN && right.GetType().id() == LogicalTypeId::BOOLEAN &&
	         result.GetType().id() == LogicalTypeId::BOOLEAN);

	if (left.GetVectorType() == VectorType::CONSTANT_VECTOR && right.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		// operation on two constants, result is constant vector
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		auto ldata = ConstantVector::GetData<uint8_t>(left);
		auto rdata = ConstantVector::GetData<uint8_t>(right);
		auto result_data = ConstantVector::GetData<bool>(result);

		bool is_null = OP::Operation(*ldata > 0, *rdata > 0, ConstantVector::IsNull(left),
		                             ConstantVector::IsNull(right), *result_data);
		ConstantVector::SetNull(result, is_null);
	} else {
		// perform generic loop
		UnifiedVectorFormat ldata, rdata;
		left.ToUnifiedFormat(count, ldata);
		right.ToUnifiedFormat(count, rdata);

		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto left_data = (uint8_t *)ldata.data; // we use uint8 to avoid load of gunk bools
		auto right_data = (uint8_t *)rdata.data;
		auto result_data = FlatVector::GetData<bool>(result);
		auto &result_mask = FlatVector::Validity(result);
		if (!ldata.validity.AllValid() || !rdata.validity.AllValid()) {
			for (idx_t i = 0; i < count; i++) {
				auto lidx = ldata.sel->get_index(i);
				auto ridx = rdata.sel->get_index(i);
				bool is_null =
				    OP::Operation(left_data[lidx] > 0, right_data[ridx] > 0, !ldata.validity.RowIsValid(lidx),
				                  !rdata.validity.RowIsValid(ridx), result_data[i]);
				result_mask.Set(i, !is_null);
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				auto lidx = ldata.sel->get_index(i);
				auto ridx = rdata.sel->get_index(i);
				result_data[i] = OP::SimpleOperation(left_data[lidx], right_data[ridx]);
			}
		}
	}
}

/*
SQL AND Rules:

TRUE  AND TRUE   = TRUE
TRUE  AND FALSE  = FALSE
TRUE  AND NULL   = NULL
FALSE AND TRUE   = FALSE
FALSE AND FALSE  = FALSE
FALSE AND NULL   = FALSE
NULL  AND TRUE   = NULL
NULL  AND FALSE  = FALSE
NULL  AND NULL   = NULL

Basically:
- Only true if both are true
- False if either is false (regardless of NULLs)
- NULL otherwise
*/
struct TernaryAnd {
	static bool SimpleOperation(bool left, bool right) {
		return left && right;
	}
	static bool Operation(bool left, bool right, bool left_null, bool right_null, bool &result) {
		if (left_null && right_null) {
			// both NULL:
			// result is NULL
			return true;
		} else if (left_null) {
			// left is NULL:
			// result is FALSE if right is false
			// result is NULL if right is true
			result = right;
			return right;
		} else if (right_null) {
			// right is NULL:
			// result is FALSE if left is false
			// result is NULL if left is true
			result = left;
			return left;
		} else {
			// no NULL: perform the AND
			result = left && right;
			return false;
		}
	}
};

void VectorOperations::And(Vector &left, Vector &right, Vector &result, idx_t count) {
	TemplatedBooleanNullmask<TernaryAnd>(left, right, result, count);
}

/*
SQL OR Rules:

OR
TRUE  OR TRUE  = TRUE
TRUE  OR FALSE = TRUE
TRUE  OR NULL  = TRUE
FALSE OR TRUE  = TRUE
FALSE OR FALSE = FALSE
FALSE OR NULL  = NULL
NULL  OR TRUE  = TRUE
NULL  OR FALSE = NULL
NULL  OR NULL  = NULL

Basically:
- Only false if both are false
- True if either is true (regardless of NULLs)
- NULL otherwise
*/

struct TernaryOr {
	static bool SimpleOperation(bool left, bool right) {
		return left || right;
	}
	static bool Operation(bool left, bool right, bool left_null, bool right_null, bool &result) {
		if (left_null && right_null) {
			// both NULL:
			// result is NULL
			return true;
		} else if (left_null) {
			// left is NULL:
			// result is TRUE if right is true
			// result is NULL if right is false
			result = right;
			return !right;
		} else if (right_null) {
			// right is NULL:
			// result is TRUE if left is true
			// result is NULL if left is false
			result = left;
			return !left;
		} else {
			// no NULL: perform the OR
			result = left || right;
			return false;
		}
	}
};

void VectorOperations::Or(Vector &left, Vector &right, Vector &result, idx_t count) {
	TemplatedBooleanNullmask<TernaryOr>(left, right, result, count);
}

struct NotOperator {
	template <class TA, class TR>
	static inline TR Operation(TA left) {
		return !left;
	}
};

void VectorOperations::Not(Vector &input, Vector &result, idx_t count) {
	D_ASSERT(input.GetType() == LogicalType::BOOLEAN && result.GetType() == LogicalType::BOOLEAN);
	UnaryExecutor::Execute<bool, bool, NotOperator>(input, result, count);
}

} // namespace duckdb
