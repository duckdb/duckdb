//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/cast_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

//! CastExpression represents a type cast from one SQL type to another SQL type
class CastExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::CAST;

public:
	DUCKDB_API CastExpression(LogicalType target, unique_ptr<ParsedExpression> child, bool try_cast = false);

public:
	const LogicalType &TargetType() const {
		return cast_type;
	}
	LogicalType &TargetTypeMutable() {
		return cast_type;
	}
	const ParsedExpression &Child() const {
		return *child;
	}
	unique_ptr<ParsedExpression> &ChildMutable() {
		return child;
	}
	bool IsTryCast() const {
		return try_cast;
	}

	string ToString() const override;

	bool Equals(const ParsedExpression &other) const override;
	hash_t Hash() const override;

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);

public:
	template <class T, class BASE>
	static string ToString(const T &entry) {
		return (entry.IsTryCast() ? "TRY_CAST(" : "CAST(") + entry.Child().ToString() + " AS " +
		       entry.TargetType().ToString() + ")";
	}

private:
	//! The child of the cast expression
	unique_ptr<ParsedExpression> child;
	//! The type to cast to
	LogicalType cast_type;
	//! Whether or not this is a try_cast expression
	bool try_cast;

private:
	CastExpression();
};
} // namespace duckdb
