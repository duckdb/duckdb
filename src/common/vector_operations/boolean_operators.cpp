//===--------------------------------------------------------------------===//
// boolean_operators.cpp
// Description: This file contains the implementation of the boolean
// operations AND OR !
//===--------------------------------------------------------------------===//

#include "duckdb/common/operator/boolean_operators.hpp"

#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

//===--------------------------------------------------------------------===//
// AND/OR
//===--------------------------------------------------------------------===//
template <class OP, class NULLOP>
static void templated_boolean_nullmask(Vector &left, Vector &right, Vector &result, idx_t count) {
	assert(left.type == TypeId::BOOL && right.type == TypeId::BOOL && result.type == TypeId::BOOL);

	if (left.vector_type == VectorType::CONSTANT_VECTOR && right.vector_type == VectorType::CONSTANT_VECTOR) {
		// operation on two constants, result is constant vector
		result.vector_type = VectorType::CONSTANT_VECTOR;
		auto ldata = ConstantVector::GetData<bool>(left);
		auto rdata = ConstantVector::GetData<bool>(right);
		auto result_data = ConstantVector::GetData<bool>(result);
		*result_data = OP::Operation(*ldata, *rdata);
		ConstantVector::SetNull(
		    result, NULLOP::Operation(*ldata, *rdata, ConstantVector::IsNull(left), ConstantVector::IsNull(right)));
	} else {
		// perform generic loop
		VectorData ldata, rdata;
		left.Orrify(count, ldata);
		right.Orrify(count, rdata);

		result.vector_type = VectorType::FLAT_VECTOR;
		auto left_data = (bool *)ldata.data;
		auto right_data = (bool *)rdata.data;
		auto result_data = FlatVector::GetData<bool>(result);
		auto &result_mask = FlatVector::Nullmask(result);
		if (ldata.nullmask->any() || rdata.nullmask->any()) {
			for (idx_t i = 0; i < count; i++) {
				auto lidx = ldata.sel->get_index(i);
				auto ridx = rdata.sel->get_index(i);
				result_data[i] = OP::Operation(left_data[lidx], right_data[ridx]);
				result_mask[i] = NULLOP::Operation(left_data[lidx], right_data[ridx], (*ldata.nullmask)[lidx],
				                                   (*rdata.nullmask)[ridx]);
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				auto lidx = ldata.sel->get_index(i);
				auto ridx = rdata.sel->get_index(i);
				result_data[i] = OP::Operation(left_data[lidx], right_data[ridx]);
			}
		}
	}
}

void VectorOperations::And(Vector &left, Vector &right, Vector &result, idx_t count) {
	templated_boolean_nullmask<duckdb::And, duckdb::AndMask>(left, right, result, count);
}

void VectorOperations::Or(Vector &left, Vector &right, Vector &result, idx_t count) {
	templated_boolean_nullmask<duckdb::Or, duckdb::OrMask>(left, right, result, count);
}

struct NotOperator {
	template <class TA, class TR> static inline TR Operation(TA left) {
		return !left;
	}
};

void VectorOperations::Not(Vector &input, Vector &result, idx_t count) {
	assert(input.type == TypeId::BOOL && result.type == TypeId::BOOL);
	UnaryExecutor::Execute<bool, bool, NotOperator>(input, result, count);
}
