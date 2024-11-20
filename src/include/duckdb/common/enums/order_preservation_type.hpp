//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/order_preservation_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Order Preservation Type
//===--------------------------------------------------------------------===//
enum class OrderPreservationType : uint8_t {
	NO_ORDER,        // the operator makes no guarantees on order preservation (i.e. it might re-order the entire input)
	INSERTION_ORDER, // the operator maintains the order of the child operators
	FIXED_ORDER      // the operator outputs rows in a fixed order that must be maintained (e.g. ORDER BY)
};

} // namespace duckdb
