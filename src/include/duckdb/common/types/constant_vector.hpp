//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/constant_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/vector.hpp"

#include <type_traits>

namespace duckdb {

//! The ConstantVector is a vector that references a single constant value
class ConstantVector : public Vector {
public:
	ConstantVector(Value value_) : value(value_) {
		Reference(value);
	}

private:
	Value value;
};

} // namespace duckdb
