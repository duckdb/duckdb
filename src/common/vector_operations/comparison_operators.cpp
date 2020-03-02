//===--------------------------------------------------------------------===//
// comparison_operators.cpp
// Description: This file contains the implementation of the comparison
// operations == != >= <= > <
//===--------------------------------------------------------------------===//

#include "duckdb/common/operator/comparison_operators.hpp"

#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

struct ComparisonExecutor {
private:
	template <class T, class OP, bool IGNORE_NULL = false>
	static inline void TemplatedExecute(Vector &left, Vector &right, Vector &result) {
		BinaryExecutor::Execute<T, T, bool, OP, IGNORE_NULL>(left, right, result);
	}

public:
	template <class OP> static inline void Execute(Vector &left, Vector &right, Vector &result) {
		assert(left.type == right.type && result.type == TypeId::BOOL);
		// the inplace loops take the result as the last parameter
		switch (left.type) {
		case TypeId::BOOL:
		case TypeId::INT8:
			TemplatedExecute<int8_t, OP>(left, right, result);
			break;
		case TypeId::INT16:
			TemplatedExecute<int16_t, OP>(left, right, result);
			break;
		case TypeId::INT32:
			TemplatedExecute<int32_t, OP>(left, right, result);
			break;
		case TypeId::INT64:
			TemplatedExecute<int64_t, OP>(left, right, result);
			break;
		case TypeId::POINTER:
			TemplatedExecute<uint64_t, OP>(left, right, result);
			break;
		case TypeId::FLOAT:
			TemplatedExecute<float, OP>(left, right, result);
			break;
		case TypeId::DOUBLE:
			TemplatedExecute<double, OP>(left, right, result);
			break;
		case TypeId::VARCHAR:
			TemplatedExecute<string_t, OP, true>(left, right, result);
			break;
		default:
			throw InvalidTypeException(left.type, "Invalid type for comparison");
		}
	}
};

void VectorOperations::Equals(Vector &left, Vector &right, Vector &result) {
	ComparisonExecutor::Execute<duckdb::Equals>(left, right, result);
}

void VectorOperations::NotEquals(Vector &left, Vector &right, Vector &result) {
	ComparisonExecutor::Execute<duckdb::NotEquals>(left, right, result);
}

void VectorOperations::GreaterThanEquals(Vector &left, Vector &right, Vector &result) {
	ComparisonExecutor::Execute<duckdb::GreaterThanEquals>(left, right, result);
}

void VectorOperations::LessThanEquals(Vector &left, Vector &right, Vector &result) {
	ComparisonExecutor::Execute<duckdb::LessThanEquals>(left, right, result);
}

void VectorOperations::GreaterThan(Vector &left, Vector &right, Vector &result) {
	ComparisonExecutor::Execute<duckdb::GreaterThan>(left, right, result);
}

void VectorOperations::LessThan(Vector &left, Vector &right, Vector &result) {
	ComparisonExecutor::Execute<duckdb::LessThan>(left, right, result);
}
