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
	JoinCondition() {
	}

	//! Turns the JoinCondition into an expression; note that this destroys the JoinCondition as the expression inherits
	//! the left/right expressions
	static unique_ptr<Expression> CreateExpression(JoinCondition cond);
	static unique_ptr<Expression> CreateExpression(vector<JoinCondition> conditions);

	void Serialize(Serializer &serializer) const;
	static JoinCondition Deserialize(Deserializer &deserializer);

public:
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
