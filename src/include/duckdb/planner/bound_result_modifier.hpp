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
	explicit BoundResultModifier(ResultModifierType type);
	virtual ~BoundResultModifier();

	ResultModifierType type;
};

struct BoundOrderByNode {
public:
	BoundOrderByNode(OrderType type, OrderByNullType null_order, unique_ptr<Expression> expression);
	BoundOrderByNode(OrderType type, OrderByNullType null_order, unique_ptr<Expression> expression,
	                 unique_ptr<BaseStatistics> stats);

	OrderType type;
	OrderByNullType null_order;
	unique_ptr<Expression> expression;
	unique_ptr<BaseStatistics> stats;

public:
	BoundOrderByNode Copy() const;
	bool Equals(const BoundOrderByNode &other) const;
	string ToString() const;

	void Serialize(Serializer &serializer) const;
	static BoundOrderByNode Deserialize(Deserializer &source, PlanDeserializationState &state);
};

class BoundLimitModifier : public BoundResultModifier {
public:
	BoundLimitModifier();

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
	BoundOrderModifier();

	//! List of order nodes
	vector<BoundOrderByNode> orders;
};

class BoundDistinctModifier : public BoundResultModifier {
public:
	BoundDistinctModifier();

	//! list of distinct on targets (if any)
	vector<unique_ptr<Expression>> target_distincts;
};

class BoundLimitPercentModifier : public BoundResultModifier {
public:
	BoundLimitPercentModifier();

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
