//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/like.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {
class LikeUtil {
public:
	static bool GetCollationLikePattern(unique_ptr<Expression> &lhs, unique_ptr<Expression> &rhs,
										LogicalType &result_type) {
		auto coll_left = StringType::GetCollation(lhs->return_type);
		auto coll_right = StringType::GetCollation(rhs->return_type);
		if (coll_left.empty() && coll_right.empty()) {
			return false;
		}
		if (coll_left == coll_right) {
			result_type = rhs->return_type;
			return true;
		}
		if (coll_left.empty()) {
			result_type = rhs->return_type;
		} else if (coll_right.empty()) {
			result_type = lhs->return_type;
		} else { // different collations not supported
			throw SyntaxException("Invalid different collations in the Like Operator.");
		}
		return true;
	}
};

} // namespace duckdb
