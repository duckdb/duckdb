//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/enums/order_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

enum class OrderType : uint8_t {
	INVALID = 0,
	ASCENDING = 1,
	DESCENDING = 2
};

}
