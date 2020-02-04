//===--------------------------------------------------------------------===//
// numeric_binary_operators.cpp
// Description: This file contains the implementation of the numeric binop
// operations (+ - / * %)
//===--------------------------------------------------------------------===//

#include "duckdb/common/operator/numeric_binary_operators.hpp"

#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

struct NumericBinaryExecutor {
private:
	template <class T, class OP, bool IGNORE_NULL, class OPWRAPPER>
	static inline void TemplatedExecute(Vector &left, Vector &right, Vector &result) {
		BinaryExecutor::Execute<T, T, T, OP, IGNORE_NULL, OPWRAPPER>(left, right, result);
	}

public:
	template <class OP, bool IGNORE_NULL = false, class OPWRAPPER = BinarySingleArgumentOperatorWrapper>
	static inline void Execute(Vector &left, Vector &right, Vector &result) {
		assert(left.type == right.type && left.type == result.type);
		switch (left.type) {
		case TypeId::INT8:
			TemplatedExecute<int8_t, OP, IGNORE_NULL, OPWRAPPER>(left, right, result);
			break;
		case TypeId::INT16:
			TemplatedExecute<int16_t, OP, IGNORE_NULL, OPWRAPPER>(left, right, result);
			break;
		case TypeId::INT32:
			TemplatedExecute<int32_t, OP, IGNORE_NULL, OPWRAPPER>(left, right, result);
			break;
		case TypeId::INT64:
			TemplatedExecute<int64_t, OP, IGNORE_NULL, OPWRAPPER>(left, right, result);
			break;
		case TypeId::FLOAT:
			TemplatedExecute<float, OP, IGNORE_NULL, OPWRAPPER>(left, right, result);
			break;
		case TypeId::DOUBLE:
			TemplatedExecute<double, OP, IGNORE_NULL, OPWRAPPER>(left, right, result);
			break;
		case TypeId::POINTER:
			TemplatedExecute<uint64_t, OP, IGNORE_NULL, OPWRAPPER>(left, right, result);
			break;
		default:
			throw InvalidTypeException(left.type, "Invalid type for numeric operator");
		}
	}
};

//===--------------------------------------------------------------------===//
// Addition
//===--------------------------------------------------------------------===//
void VectorOperations::Add(Vector &left, Vector &right, Vector &result) {
	NumericBinaryExecutor::Execute<duckdb::Add>(left, right, result);
}

//===--------------------------------------------------------------------===//
// Subtraction
//===--------------------------------------------------------------------===//
void VectorOperations::Subtract(Vector &left, Vector &right, Vector &result) {
	NumericBinaryExecutor::Execute<duckdb::Subtract>(left, right, result);
}

//===--------------------------------------------------------------------===//
// Multiplication
//===--------------------------------------------------------------------===//
void VectorOperations::Multiply(Vector &left, Vector &right, Vector &result) {
	NumericBinaryExecutor::Execute<duckdb::Multiply>(left, right, result);
}

//===--------------------------------------------------------------------===//
// Divide
//===--------------------------------------------------------------------===//
struct BinaryZeroIsNullWrapper {
	template <class FUNC, class OP, class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, LEFT_TYPE left, RIGHT_TYPE right, nullmask_t &nullmask, index_t idx) {
		if (right == 0) {
			nullmask[idx] = true;
			return 0;
		} else {
			return OP::template Operation<LEFT_TYPE>(left, right);
		}
	}
};

void VectorOperations::Divide(Vector &left, Vector &right, Vector &result) {
	NumericBinaryExecutor::Execute<duckdb::Divide, true, BinaryZeroIsNullWrapper>(left, right, result);
}

//===--------------------------------------------------------------------===//
// Modulo
//===--------------------------------------------------------------------===//
template <> float Modulo::Operation(float left, float right) {
	assert(right != 0);
	return fmod(left, right);
}

template <> double Modulo::Operation(double left, double right) {
	assert(right != 0);
	return fmod(left, right);
}

void VectorOperations::Modulo(Vector &left, Vector &right, Vector &result) {
	NumericBinaryExecutor::Execute<duckdb::Modulo, true, BinaryZeroIsNullWrapper>(left, right, result);
}
