//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/joinside.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

//! JoinCondition represents a left-right comparison join condition
struct JoinCondition {
public:
	JoinCondition(unique_ptr<Expression> lhs, unique_ptr<Expression> rhs, ExpressionType comparison)
	    : left(std::move(lhs)), right(std::move(rhs)), comparison(comparison) {
	}

	explicit JoinCondition(unique_ptr<Expression> join_condition)
	    : left(std::move(join_condition)), comparison(ExpressionType::INVALID) {
	}

	JoinCondition() : comparison(ExpressionType::INVALID) {
	}

	bool IsComparison() const {
		return comparison != ExpressionType::INVALID;
	}

	JoinCondition Copy() const {
		if (IsComparison()) {
			return JoinCondition(left->Copy(), right->Copy(), comparison);
		}
		return JoinCondition(left->Copy());
	}

	Expression &GetLHS() {
		if (!IsComparison()) {
			throw InternalException("GetLHS used on a JoinCondition that is not a left/right comparison");
		}
		return *left;
	}

	const Expression &GetLHS() const {
		if (!IsComparison()) {
			throw InternalException("GetLHS used on a JoinCondition that is not a left/right comparison");
		}
		return *left;
	}

	Expression &GetRHS() {
		if (!IsComparison()) {
			throw InternalException("GetRHS used on a JoinCondition that is not a left/right comparison");
		}
		return *right;
	}

	const Expression &GetRHS() const {
		if (!IsComparison()) {
			throw InternalException("GetRHS used on a JoinCondition that is not a left/right comparison");
		}
		return *right;
	}

	ExpressionType GetComparisonType() const {
		if (!IsComparison()) {
			throw InternalException("GetComparisonType used on a JoinCondition that is not a left/right comparison");
		}
		return comparison;
	}

	void Swap() {
		if (!IsComparison()) {
			throw InternalException("Swap used on a JoinCondition that is not a left/right comparison");
		}
		std::swap(left, right);
		comparison = FlipComparisonExpression(comparison);
	}

	unique_ptr<Expression> &LeftReference() {
		if (!IsComparison()) {
			throw InternalException("LeftReference used on a JoinCondition that is not a left/right comparison");
		}
		return left;
	}

	unique_ptr<Expression> &RightReference() {
		if (!IsComparison()) {
			throw InternalException("RightReference used on a JoinCondition that is not a left/right comparison");
		}
		return right;
	}

	Expression &GetJoinExpression() {
		if (IsComparison()) {
			throw InternalException("GetJoinExpression used on a JoinCondition that is a comparison");
		}
		return *left;
	}

	const Expression &GetJoinExpression() const {
		if (IsComparison()) {
			throw InternalException("GetJoinExpression used on a JoinCondition that is a comparison");
		}
		return *left;
	}

	unique_ptr<Expression> &JoinExpressionReference() {
		if (IsComparison()) {
			throw InternalException("JoinExpressionReference used on a JoinCondition that is a comparison");
		}
		return left;
	}

	const unique_ptr<Expression> &JoinExpressionReference() const {
		if (IsComparison()) {
			throw InternalException("JoinExpressionReference used on a JoinCondition that is a comparison");
		}
		return left;
	}

	//! Turns the JoinCondition into an expression; note that this destroys the JoinCondition as the expression inherits
	//! the left/right expressions
	static unique_ptr<Expression> CreateExpression(JoinCondition cond);
	static unique_ptr<Expression> CreateExpression(vector<JoinCondition> conditions);

	void Serialize(Serializer &serializer) const;
	static JoinCondition Deserialize(Deserializer &deserializer);

private:
	unique_ptr<Expression> left;
	unique_ptr<Expression> right;
	ExpressionType comparison;
};

class JoinSide {
public:
	enum JoinValue : uint8_t { NONE, LEFT, RIGHT, BOTH };

	JoinSide() = default;
	constexpr JoinSide(JoinValue val) : value(val) { // NOLINT: Allow implicit conversion from `join_value`
	}

	bool operator==(JoinSide a) const {
		return value == a.value;
	}
	bool operator!=(JoinSide a) const {
		return value != a.value;
	}

	static JoinSide CombineJoinSide(JoinSide left, JoinSide right);
	static JoinSide GetJoinSide(idx_t table_binding, const unordered_set<idx_t> &left_bindings,
	                            const unordered_set<uint64_t> &right_bindings);
	static JoinSide GetJoinSide(Expression &expression, const unordered_set<idx_t> &left_bindings,
	                            const unordered_set<idx_t> &right_bindings);
	static JoinSide GetJoinSide(const unordered_set<idx_t> &bindings, const unordered_set<idx_t> &left_bindings,
	                            const unordered_set<idx_t> &right_bindings);

private:
	JoinValue value;
};

} // namespace duckdb
