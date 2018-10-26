//===--------------------------------------------------------------------===//
// conjunction_operators.cpp
// Description: This file contains the implementation of the conjunction
// operations AND OR
//===--------------------------------------------------------------------===//

#include "common/operator/conjunction_operators.hpp"
#include "common/types/vector_operations.hpp"
#include "common/vector_operations/binary_loops.hpp"

using namespace duckdb;
using namespace std;

//===--------------------------------------------------------------------===//
// AND/OR
//===--------------------------------------------------------------------===//
template <class OP, class NULLOP>
void templated_boolean_nullmask(Vector &left, Vector &right, Vector &result) {
	if (left.type != TypeId::BOOLEAN || right.type != TypeId::BOOLEAN) {
		throw TypeMismatchException(
		    left.type, right.type,
		    "Conjunction can only be applied on boolean values");
	}

	auto ldata = (bool *)left.data;
	auto rdata = (bool *)right.data;
	auto result_data = (bool *)result.data;

	if (left.IsConstant()) {
		bool left_null = left.nullmask[0];
		bool constant = ldata[0];
		VectorOperations::Exec(right, [&](size_t i, size_t k) {
			result_data[i] = OP::Operation(constant, rdata[i]);
			result.nullmask[i] = NULLOP::Operation(
			    constant, rdata[i], left_null, right.nullmask[i]);
		});
		result.sel_vector = right.sel_vector;
		result.count = right.count;
	} else if (right.IsConstant()) {
		// AND / OR operations are commutative
		templated_boolean_nullmask<OP, NULLOP>(right, left, result);
	} else if (left.count == right.count) {
		assert(left.sel_vector == right.sel_vector);
		VectorOperations::Exec(left, [&](size_t i, size_t k) {
			result_data[i] = OP::Operation(ldata[i], rdata[i]);
			result.nullmask[i] = NULLOP::Operation(
			    ldata[i], rdata[i], left.nullmask[i], right.nullmask[i]);
		});
		result.sel_vector = left.sel_vector;
		result.count = left.count;
	} else {
		throw Exception("Vector lengths don't match");
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

namespace operators {
struct AndMask {
	static inline bool Operation(bool left, bool right, bool left_null,
	                             bool right_null) {
		return (left_null && (right_null || right)) || (right_null && left);
	}
};
} // namespace operators

void VectorOperations::And(Vector &left, Vector &right, Vector &result) {
	templated_boolean_nullmask<operators::And, operators::AndMask>(left, right,
	                                                               result);
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
namespace operators {
struct OrMask {
	static inline bool Operation(bool left, bool right, bool left_null,
	                             bool right_null) {
		return (left_null && (right_null || !right)) || (right_null && !left);
	}
};
} // namespace operators
void VectorOperations::Or(Vector &left, Vector &right, Vector &result) {
	templated_boolean_nullmask<operators::Or, operators::OrMask>(left, right,
	                                                             result);
}
