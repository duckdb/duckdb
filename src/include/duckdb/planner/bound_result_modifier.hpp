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

public:
	template <class TARGET>
	TARGET &Cast() {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast result modifier to type - result modifier type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast result modifier to type - result modifier type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct BoundOrderByNode {
public:
	static constexpr const ResultModifierType TYPE = ResultModifierType::ORDER_MODIFIER;

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
	static BoundOrderByNode Deserialize(Deserializer &deserializer);
};

class BoundLimitModifier : public BoundResultModifier {
public:
	static constexpr const ResultModifierType TYPE = ResultModifierType::LIMIT_MODIFIER;

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
	static constexpr const ResultModifierType TYPE = ResultModifierType::ORDER_MODIFIER;

public:
	BoundOrderModifier();

	//! List of order nodes
	vector<BoundOrderByNode> orders;

	unique_ptr<BoundOrderModifier> Copy() const;
	static bool Equals(const BoundOrderModifier &left, const BoundOrderModifier &right);
	static bool Equals(const unique_ptr<BoundOrderModifier> &left, const unique_ptr<BoundOrderModifier> &right);

	void Serialize(Serializer &serializer) const;
	static unique_ptr<BoundOrderModifier> Deserialize(Deserializer &deserializer);
};

enum class DistinctType : uint8_t { DISTINCT = 0, DISTINCT_ON = 1 };

class BoundDistinctModifier : public BoundResultModifier {
public:
	static constexpr const ResultModifierType TYPE = ResultModifierType::DISTINCT_MODIFIER;

public:
	BoundDistinctModifier();

	//! Whether or not this is a DISTINCT or DISTINCT ON
	DistinctType distinct_type;
	//! list of distinct on targets
	vector<unique_ptr<Expression>> target_distincts;
};

class BoundLimitPercentModifier : public BoundResultModifier {
public:
	static constexpr const ResultModifierType TYPE = ResultModifierType::LIMIT_PERCENT_MODIFIER;

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
