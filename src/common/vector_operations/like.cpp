//===--------------------------------------------------------------------===//
// comparison_operators.cpp
// Description: This file contains the implementation of (NOT) LIKE
//===--------------------------------------------------------------------===//

#include "duckdb/common/operator/like_operators.hpp"
#include "duckdb/common/vector_operations/binary_loops.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

template <class OP> void templated_like(Vector &left, Vector &right, Vector &result) {
	if (left.type != TypeId::VARCHAR) {
		throw InvalidTypeException(left.type, "Input of (NOT) LIKE must be VARCHAR");
	}
	if (right.type != TypeId::VARCHAR) {
		throw InvalidTypeException(right.type, "Input of (NOT) LIKE must be VARCHAR");
	}
	if (result.type != TypeId::BOOLEAN) {
		throw InvalidTypeException(result.type, "Result of (NOT) LIKE must be VARCHAR");
	}
	templated_binary_loop<const char *, const char *, bool, OP, true>(left, right, result);
}

void VectorOperations::Like(Vector &left, Vector &right, Vector &result) {
	templated_like<duckdb::Like>(left, right, result);
}

void VectorOperations::NotLike(Vector &left, Vector &right, Vector &result) {
	templated_like<duckdb::NotLike>(left, right, result);
}
