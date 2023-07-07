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
class FieldWriter;
class FieldReader;
class FormatDeserializer;
class FormatSerializer;

enum class ResultModifierType : uint8_t {
	LIMIT_MODIFIER = 1,
	ORDER_MODIFIER = 2,
	DISTINCT_MODIFIER = 3,
	LIMIT_PERCENT_MODIFIER = 4
};

const char *ToString(ResultModifierType value);
ResultModifierType ResultModifierFromString(const char *value);

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
	virtual bool Equals(const ResultModifier &other) const;

	//! Create a copy of this ResultModifier
	virtual unique_ptr<ResultModifier> Copy() const = 0;
	//! Serializes a ResultModifier to a stand-alone binary blob
	void Serialize(Serializer &serializer) const;
	//! Serializes a ResultModifier to a stand-alone binary blob
	virtual void Serialize(FieldWriter &writer) const = 0;
	//! Deserializes a blob back into a ResultModifier
	static unique_ptr<ResultModifier> Deserialize(Deserializer &source);

	virtual void FormatSerialize(FormatSerializer &serializer) const;
	static unique_ptr<ResultModifier> FormatDeserialize(FormatDeserializer &deserializer);

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

//! Single node in ORDER BY statement
struct OrderByNode {
	OrderByNode(OrderType type, OrderByNullType null_order, unique_ptr<ParsedExpression> expression)
	    : type(type), null_order(null_order), expression(std::move(expression)) {
	}

	//! Sort order, ASC or DESC
	OrderType type;
	//! The NULL sort order, NULLS_FIRST or NULLS_LAST
	OrderByNullType null_order;
	//! Expression to order by
	unique_ptr<ParsedExpression> expression;

public:
	void Serialize(Serializer &serializer) const;
	string ToString() const;
	static OrderByNode Deserialize(Deserializer &source);

	void FormatSerialize(FormatSerializer &serializer) const;
	static OrderByNode FormatDeserialize(FormatDeserializer &deserializer);
};

class LimitModifier : public ResultModifier {
public:
	static constexpr const ResultModifierType TYPE = ResultModifierType::LIMIT_MODIFIER;

public:
	LimitModifier() : ResultModifier(ResultModifierType::LIMIT_MODIFIER) {
	}

	//! LIMIT count
	unique_ptr<ParsedExpression> limit;
	//! OFFSET
	unique_ptr<ParsedExpression> offset;

public:
	bool Equals(const ResultModifier &other) const override;
	unique_ptr<ResultModifier> Copy() const override;
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<ResultModifier> Deserialize(FieldReader &reader);

	void FormatSerialize(FormatSerializer &serializer) const override;
	static unique_ptr<ResultModifier> FormatDeserialize(FormatDeserializer &deserializer);
};

class OrderModifier : public ResultModifier {
public:
	static constexpr const ResultModifierType TYPE = ResultModifierType::ORDER_MODIFIER;

public:
	OrderModifier() : ResultModifier(ResultModifierType::ORDER_MODIFIER) {
	}

	//! List of order nodes
	vector<OrderByNode> orders;

public:
	bool Equals(const ResultModifier &other) const override;
	unique_ptr<ResultModifier> Copy() const override;
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<ResultModifier> Deserialize(FieldReader &reader);

	void FormatSerialize(FormatSerializer &serializer) const override;
	static unique_ptr<ResultModifier> FormatDeserialize(FormatDeserializer &deserializer);

	static bool Equals(const unique_ptr<OrderModifier> &left, const unique_ptr<OrderModifier> &right);
};

class DistinctModifier : public ResultModifier {
public:
	static constexpr const ResultModifierType TYPE = ResultModifierType::DISTINCT_MODIFIER;

public:
	DistinctModifier() : ResultModifier(ResultModifierType::DISTINCT_MODIFIER) {
	}

	//! list of distinct on targets (if any)
	vector<unique_ptr<ParsedExpression>> distinct_on_targets;

public:
	bool Equals(const ResultModifier &other) const override;
	unique_ptr<ResultModifier> Copy() const override;
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<ResultModifier> Deserialize(FieldReader &reader);

	void FormatSerialize(FormatSerializer &serializer) const override;
	static unique_ptr<ResultModifier> FormatDeserialize(FormatDeserializer &deserializer);
};

class LimitPercentModifier : public ResultModifier {
public:
	static constexpr const ResultModifierType TYPE = ResultModifierType::LIMIT_PERCENT_MODIFIER;

public:
	LimitPercentModifier() : ResultModifier(ResultModifierType::LIMIT_PERCENT_MODIFIER) {
	}

	//! LIMIT %
	unique_ptr<ParsedExpression> limit;
	//! OFFSET
	unique_ptr<ParsedExpression> offset;

public:
	bool Equals(const ResultModifier &other) const override;
	unique_ptr<ResultModifier> Copy() const override;
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<ResultModifier> Deserialize(FieldReader &reader);

	void FormatSerialize(FormatSerializer &serializer) const override;
	static unique_ptr<ResultModifier> FormatDeserialize(FormatDeserializer &deserializer);
};

} // namespace duckdb
