//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/debug_vector_verification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//- vector_type: "dictionary_expression"
//  test_parameter: '"*"'
//- vector_type: "dictionary_operator"
//  test_parameter: '' # skip running all tests here because it is too slow
//- vector_type: "constant_operator"
//  test_parameter: '' # skip running all tests here because it is too slow
//- vector_type: "sequence_operator"
//  test_parameter: '' # skip running all tests here because it is too slow
//- vector_type: "nested_shuffle"
//  test_parameter: '' # skip running all tests here because it is too slow

enum class DebugVectorVerification : uint8_t {
	NONE,
	DICTIONARY_EXPRESSION,
	DICTIONARY_OPERATOR,
	CONSTANT_OPERATOR,
	SEQUENCE_OPERATOR,
	NESTED_SHUFFLE
};

} // namespace duckdb
