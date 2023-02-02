//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/order_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class OrderType : uint8_t { INVALID = 0, ORDER_DEFAULT = 1, ASCENDING = 2, DESCENDING = 3 };
inline const char *ToString(OrderType value) {
	switch (value) {
	case OrderType::INVALID:
		return "INVALID";
	case OrderType::ORDER_DEFAULT:
		return "ORDER_DEFAULT";
	case OrderType::ASCENDING:
		return "ASCENDING";
	case OrderType::DESCENDING:
		return "DESCENDING";
	}
}

enum class OrderByNullType : uint8_t { INVALID = 0, ORDER_DEFAULT = 1, NULLS_FIRST = 2, NULLS_LAST = 3 };
inline const char *ToString(OrderByNullType value) {
	switch (value) {
	case OrderByNullType::INVALID:
		return "INVALID";
	case OrderByNullType::ORDER_DEFAULT:
		return "ORDER_DEFAULT";
	case OrderByNullType::NULLS_FIRST:
		return "NULLS_FIRST";
	case OrderByNullType::NULLS_LAST:
		return "NULLS_LAST";
	}
}

} // namespace duckdb
