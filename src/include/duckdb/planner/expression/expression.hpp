//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/parser/base_expression.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/planner/plan_serialization.hpp"
#include "duckdb/optimizer/cascade/types.h"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {
using namespace gpos;

class Expression;
class BaseStatistics;
class FieldWriter;
class FieldReader;
class ClientContext;

//!  The Expression class represents a bound Expression with a return type
class Expression : public BaseExpression {
public:
	Expression(ExpressionType type, ExpressionClass expression_class, LogicalType return_type);
	~Expression() override;

	//! The return type of the expression
	LogicalType return_type;
	
	//! Expression statistics (if any) - ONLY USED FOR VERIFICATION
	unique_ptr<BaseStatistics> verification_stats;

public:
	bool IsAggregate() const override;
	bool IsWindow() const override;
	bool HasSubquery() const override;
	bool IsScalar() const override;
	bool HasParameter() const override;
	virtual bool HasSideEffects() const;
	virtual bool PropagatesNullValues() const;
	virtual bool IsFoldable() const;

	static ULONG HashValue(const Expression* exp);
	
	hash_t Hash() const override;

	bool Equals(const BaseExpression *other) const override {
		if (!BaseExpression::Equals(other))
		{
			return false;
		}
		return return_type == ((Expression *)other)->return_type;
	}

	static bool Equals(const Expression *left, const Expression *right) {
		return BaseExpression::Equals((const BaseExpression *)left, (const BaseExpression *)right);
	}
	
	static bool Equals(const Expression &left, const Expression &right) {
		return left.Equals(&right);
	}
	
	//! Create a copy of this expression
	virtual unique_ptr<Expression> Copy() = 0;

	//! Serializes an Expression to a stand-alone binary blob
	void Serialize(Serializer &serializer) const;
	
	//! Serializes an Expression to a stand-alone binary blob
	virtual void Serialize(FieldWriter &writer) const = 0;

	//! Deserializes a blob back into an Expression [CAN THROW:
	//! SerializationException]
	static unique_ptr<Expression> Deserialize(Deserializer &source, PlanDeserializationState &state);

protected:
	//! Copy base Expression properties from another expression to this one,
	//! used in Copy method
	void CopyProperties(Expression &other)
	{
		type = other.type;
		expression_class = other.expression_class;
		alias = other.alias;
		return_type = other.return_type;
	}

public:
	// return constraint on a given column
	virtual Expression* Pcnstr(ColumnBinding colref)
	{
		return NULL;
	}

	// return constraint on a given set of columns
	virtual Expression* Pcnstr(vector<ColumnBinding> pcrs)
	{
		return NULL;
	}

public:
	virtual vector<ColumnBinding> getColumnBinding()
	{
		vector<ColumnBinding> v;
		return v;
	}
};

} // namespace duckdb
