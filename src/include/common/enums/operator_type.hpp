//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/enums/operator_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Set of built-in operators
//===--------------------------------------------------------------------===//
enum class OperatorType : uint8_t {
	NONE = 0, // not a built-in operator
	ADD = 1,
	SUBTRACT = 2,
	MULTIPLY = 3,
	DIVIDE = 4,
	MOD = 5,
	BITWISE_LSHIFT = 6,
	BITWISE_RSHIFT = 7,
	BITWISE_AND = 8,
	BITWISE_OR = 9,
	BITWISE_XOR = 10
};

string OperatorTypeToOperator(OperatorType type);
OperatorType OperatorTypeFromOperator(string op);

} // namespace duckdb
