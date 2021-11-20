//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/bound_result_modifier.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/limits.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/planner/bound_statement.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

//! A ResultModifier
class BoundResultModifier {
public:
	explicit BoundResultModifier(ResultModifierType type) : type(type) {
	}
	virtual ~BoundResultModifier() {
	}

	ResultModifierType type;
};

struct BoundOrderByNode {
public:
	BoundOrderByNode(OrderType type, OrderByNullType null_order, unique_ptr<Expression> expression)
	    : type(type), null_order(null_order), expression(move(expression)) {
	}
	BoundOrderByNode(OrderType type, OrderByNullType null_order, unique_ptr<Expression> expression,
	                 unique_ptr<BaseStatistics> stats)
	    : type(type), null_order(null_order), expression(move(expression)), stats(move(stats)) {
	}

	BoundOrderByNode Copy() const {
		if (stats) {
			return BoundOrderByNode(type, null_order, expression->Copy(), stats->Copy());
		} else {
			return BoundOrderByNode(type, null_order, expression->Copy());
		}
	}

	OrderType type;
	OrderByNullType null_order;
	unique_ptr<Expression> expression;
	unique_ptr<BaseStatistics> stats;
};

class BoundLimitModifier : public BoundResultModifier {
public:
	BoundLimitModifier() : BoundResultModifier(ResultModifierType::LIMIT_MODIFIER) {
	}
	//! LIMIT
	int64_t limit_val = NumericLimits<int64_t>::Maximum();
	//! OFFSET
	int64_t offset_val = 0;
	//! Expression in case limit is not constant
	unique_ptr<Expression> limit;
	//! Expression in case limit is not constant
	unique_ptr<Expression> offset;
};

class BoundOrderModifier : public BoundResultModifier {
public:
	BoundOrderModifier() : BoundResultModifier(ResultModifierType::ORDER_MODIFIER) {
	}

	//! List of order nodes
	vector<BoundOrderByNode> orders;
};

class BoundDistinctModifier : public BoundResultModifier {
public:
	BoundDistinctModifier() : BoundResultModifier(ResultModifierType::DISTINCT_MODIFIER) {
	}

	//! list of distinct on targets (if any)
	vector<unique_ptr<Expression>> target_distincts;
};

class BoundLimitPercentModifier : public BoundResultModifier {
public:
	BoundLimitPercentModifier() : BoundResultModifier(ResultModifierType::LIMIT_PERCENT_MODIFIER) {
	}
	//! LIMIT %
	double limit_percent = 100.0;
	//! OFFSET
	int64_t offset_val = 0;
	//! Expression in case limit is not constant
	unique_ptr<Expression> limit;
	//! Expression in case limit is not constant
	unique_ptr<Expression> offset;
};

} // namespace duckdb
