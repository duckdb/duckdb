//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/result_modifier.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/enums/order_type.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

enum ResultModifierType : uint8_t {
	LIMIT_MODIFIER = 1,
	ORDER_MODIFIER = 2,
	DISTINCT_MODIFIER = 3,
	LIMIT_PERCENT_MODIFIER = 4
};

//! A ResultModifier
class ResultModifier {
public:
	explicit ResultModifier(ResultModifierType type) : type(type) {
	}
	virtual ~ResultModifier() {
	}

	ResultModifierType type;

public:
	//! Returns true if the two result modifiers are equivalent
	virtual bool Equals(const ResultModifier *other) const;

	//! Create a copy of this ResultModifier
	virtual unique_ptr<ResultModifier> Copy() = 0;
	//! Serializes a ResultModifier to a stand-alone binary blob
	virtual void Serialize(Serializer &serializer);
	//! Deserializes a blob back into a ResultModifier
	static unique_ptr<ResultModifier> Deserialize(Deserializer &source);
};

//! Single node in ORDER BY statement
struct OrderByNode {
	OrderByNode(OrderType type, OrderByNullType null_order, unique_ptr<ParsedExpression> expression)
	    : type(type), null_order(null_order), expression(move(expression)) {
	}

	//! Sort order, ASC or DESC
	OrderType type;
	//! The NULL sort order, NULLS_FIRST or NULLS_LAST
	OrderByNullType null_order;
	//! Expression to order by
	unique_ptr<ParsedExpression> expression;

public:
	void Serialize(Serializer &serializer);
	string ToString() const;
	static OrderByNode Deserialize(Deserializer &source);
};

class LimitModifier : public ResultModifier {
public:
	LimitModifier() : ResultModifier(ResultModifierType::LIMIT_MODIFIER) {
	}

	//! LIMIT count
	unique_ptr<ParsedExpression> limit;
	//! OFFSET
	unique_ptr<ParsedExpression> offset;

public:
	bool Equals(const ResultModifier *other) const override;
	unique_ptr<ResultModifier> Copy() override;
	void Serialize(Serializer &serializer) override;
	static unique_ptr<ResultModifier> Deserialize(Deserializer &source);
};

class OrderModifier : public ResultModifier {
public:
	OrderModifier() : ResultModifier(ResultModifierType::ORDER_MODIFIER) {
	}

	//! List of order nodes
	vector<OrderByNode> orders;

public:
	bool Equals(const ResultModifier *other) const override;
	unique_ptr<ResultModifier> Copy() override;
	void Serialize(Serializer &serializer) override;
	static unique_ptr<ResultModifier> Deserialize(Deserializer &source);
};

class DistinctModifier : public ResultModifier {
public:
	DistinctModifier() : ResultModifier(ResultModifierType::DISTINCT_MODIFIER) {
	}

	//! list of distinct on targets (if any)
	vector<unique_ptr<ParsedExpression>> distinct_on_targets;

public:
	bool Equals(const ResultModifier *other) const override;
	unique_ptr<ResultModifier> Copy() override;
	void Serialize(Serializer &serializer) override;
	static unique_ptr<ResultModifier> Deserialize(Deserializer &source);
};

class LimitPercentModifier : public ResultModifier {
public:
	LimitPercentModifier() : ResultModifier(ResultModifierType::LIMIT_PERCENT_MODIFIER) {
	}

	//! LIMIT %
	unique_ptr<ParsedExpression> limit;
	//! OFFSET
	unique_ptr<ParsedExpression> offset;

public:
	bool Equals(const ResultModifier *other) const override;
	unique_ptr<ResultModifier> Copy() override;
	void Serialize(Serializer &serializer) override;
	static unique_ptr<ResultModifier> Deserialize(Deserializer &source);
};

} // namespace duckdb
