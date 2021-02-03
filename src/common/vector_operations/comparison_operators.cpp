//===--------------------------------------------------------------------===//
// comparison_operators.cpp
// Description: This file contains the implementation of the comparison
// operations == != >= <= > <
//===--------------------------------------------------------------------===//

#include "duckdb/common/operator/comparison_operators.hpp"

#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

struct ComparisonExecutor {
private:
	template <class T, class OP, bool IGNORE_NULL = false>
	static inline void TemplatedExecute(Vector &left, Vector &right, Vector &result, idx_t count) {
		BinaryExecutor::Execute<T, T, bool, OP, IGNORE_NULL>(left, right, result, count);
	}

		template <class T, class OP, bool IGNORE_NULL = false>
	static inline void TemplatedDistinctExecute(Vector &left, Vector &right, Vector &result, idx_t count) {
		BinaryExecutor::DistinctExecute<T, T, bool, OP, IGNORE_NULL>(left, right, result, count);
	}

public:
	template <class OP> static inline void Execute(Vector &left, Vector &right, Vector &result, idx_t count) {
		D_ASSERT(left.type == right.type && result.type == LogicalType::BOOLEAN);
		// the inplace loops take the result as the last parameter
		switch (left.type.InternalType()) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
			TemplatedExecute<int8_t, OP>(left, right, result, count);
			break;
		case PhysicalType::INT16:
			TemplatedExecute<int16_t, OP>(left, right, result, count);
			break;
		case PhysicalType::INT32:
			TemplatedExecute<int32_t, OP>(left, right, result, count);
			break;
		case PhysicalType::INT64:
			TemplatedExecute<int64_t, OP>(left, right, result, count);
			break;
		case PhysicalType::UINT8:
			TemplatedExecute<uint8_t, OP>(left, right, result, count);
			break;
		case PhysicalType::UINT16:
			TemplatedExecute<uint16_t, OP>(left, right, result, count);
			break;
		case PhysicalType::UINT32:
			TemplatedExecute<uint32_t, OP>(left, right, result, count);
			break;
		case PhysicalType::UINT64:
			TemplatedExecute<uint64_t, OP>(left, right, result, count);
			break;
		case PhysicalType::INT128:
			TemplatedExecute<hugeint_t, OP>(left, right, result, count);
			break;
		case PhysicalType::POINTER:
			TemplatedExecute<uintptr_t, OP>(left, right, result, count);
			break;
		case PhysicalType::FLOAT:
			TemplatedExecute<float, OP>(left, right, result, count);
			break;
		case PhysicalType::DOUBLE:
			TemplatedExecute<double, OP>(left, right, result, count);
			break;
		case PhysicalType::INTERVAL:
			TemplatedExecute<interval_t, OP>(left, right, result, count);
			break;
		case PhysicalType::VARCHAR:
			TemplatedExecute<string_t, OP, true>(left, right, result, count);
			break;
		default:
			throw InvalidTypeException(left.type, "Invalid type for comparison");
		}
	}

	template <class OP> static inline void ExecuteDistinct(Vector &left, Vector &right, Vector &result, idx_t count) {
		D_ASSERT(left.type == right.type && result.type == LogicalType::BOOLEAN);
		// the inplace loops take the result as the last parameter
		switch (left.type.InternalType()) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
			TemplatedDistinctExecute<int8_t, OP>(left, right, result, count);
			break;
		case PhysicalType::INT16:
			TemplatedDistinctExecute<int16_t, OP>(left, right, result, count);
			break;
		case PhysicalType::INT32:
			TemplatedDistinctExecute<int32_t, OP>(left, right, result, count);
			break;
		case PhysicalType::INT64:
			TemplatedDistinctExecute<int64_t, OP>(left, right, result, count);
			break;
		case PhysicalType::UINT8:
			TemplatedDistinctExecute<uint8_t, OP>(left, right, result, count);
			break;
		case PhysicalType::UINT16:
			TemplatedDistinctExecute<uint16_t, OP>(left, right, result, count);
			break;
		case PhysicalType::UINT32:
			TemplatedDistinctExecute<uint32_t, OP>(left, right, result, count);
			break;
		case PhysicalType::UINT64:
			TemplatedDistinctExecute<uint64_t, OP>(left, right, result, count);
			break;
		case PhysicalType::INT128:
			TemplatedDistinctExecute<hugeint_t, OP>(left, right, result, count);
			break;
		case PhysicalType::POINTER:
			TemplatedDistinctExecute<uintptr_t, OP>(left, right, result, count);
			break;
		case PhysicalType::FLOAT:
			TemplatedDistinctExecute<float, OP>(left, right, result, count);
			break;
		case PhysicalType::DOUBLE:
			TemplatedDistinctExecute<double, OP>(left, right, result, count);
			break;
		default:
			throw InvalidTypeException(left.type, "Invalid type for distinct comparison");
		}
	}
};



void VectorOperations::Equals(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparisonExecutor::Execute<duckdb::Equals>(left, right, result, count);
}

void VectorOperations::NotEquals(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparisonExecutor::Execute<duckdb::NotEquals>(left, right, result, count);
}

void VectorOperations::GreaterThanEquals(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparisonExecutor::Execute<duckdb::GreaterThanEquals>(left, right, result, count);
}

void VectorOperations::LessThanEquals(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparisonExecutor::Execute<duckdb::LessThanEquals>(left, right, result, count);
}

void VectorOperations::GreaterThan(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparisonExecutor::Execute<duckdb::GreaterThan>(left, right, result, count);
}

void VectorOperations::LessThan(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparisonExecutor::Execute<duckdb::LessThan>(left, right, result, count);
}

void VectorOperations::DistinctFrom(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparisonExecutor::ExecuteDistinct<duckdb::DistinctFrom>(left, right, result, count);
}

void VectorOperations::NotDistinctFrom(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparisonExecutor::ExecuteDistinct<duckdb::NotDistinctFrom>(left, right, result, count);
}

} // namespace duckdb
