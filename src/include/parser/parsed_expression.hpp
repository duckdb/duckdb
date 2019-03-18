//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/parsed_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"

#include <functional>
#include <memory>
#include <stack>
#include <vector>

namespace duckdb {
class Serializer;
class Deserializer;

//!  The ParsedExpression class is a base class that can represent any expression
//!  part of a SQL statement.
/*!
 The ParsedExpression class is a base class that can represent any expression
 part of a SQL statement. This is, for example, a column reference in a SELECT
 clause, but also operators, aggregates or filters. The Expression is emitted by the parser and does not contain any information about bindings to the catalog or to the types. ParsedExpressions are transformed into regular Expressions in the Binder.
 */
class ParsedExpression {
public:
	//! Create an Expression
	ParsedExpression(ExpressionType type, ExpressionClass expression_class) :
		type(type), expression_class(expression_class) {
	}
	virtual ~ParsedExpression() {
	}

	//! Returns true if this Expression is an aggregate or not.
	/*!
	 Examples:

	 (1) SUM(a) + 1 -- True

	 (2) a + 1 -- False
	 */
	virtual bool IsAggregate();

	//! Returns true if the expression has a window function or not
	virtual bool IsWindow();

	//! Returns true if the query contains a subquery
	virtual bool HasSubquery();

	//! Returns true if expression does not contain a group ref or col ref or parameter
	virtual bool IsScalar();

	//! Returns true if the expression has a parameter
	virtual bool HasParameter();

	//! Creates a hash value of this expression. It is important that if two expressions are identical (i.e.
	//! Expression::Equals() returns true), that their hash value is identical as well.
	virtual uint64_t Hash() const;
	//! Returns true if this expression is equal to another expression
	virtual bool Equals(const ParsedExpression *other) const;

	bool operator==(const ParsedExpression &rhs) {
		return this->Equals(&rhs);
	}

	virtual string GetName() const {
		return !alias.empty() ? alias : "Unknown";
	}

	//! Returns the type of the expression
	ExpressionType GetExpressionType() {
		return type;
	}
	//! Returns the class of the expression
	ExpressionClass GetExpressionClass() {
		return expression_class;
	}

	//! Create a copy of this expression
	virtual unique_ptr<ParsedExpression> Copy() = 0;

	//! Serializes an Expression to a stand-alone binary blob
	virtual void Serialize(Serializer &serializer);
	//! Deserializes a blob back into an Expression [CAN THROW:
	//! SerializationException]
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &source);

	//! Enumerate over all children of this node, invoking the callback for each child. This method allows replacing the
	//! children using the return value. [NOTE: inside the callback, the current Expression can be corrupted as the
	//! children are moved away leaving the expression with NULL children where non-NULL children are expected!]
	void EnumerateChildren(std::function<unique_ptr<ParsedExpression>(unique_ptr<ParsedExpression> expression)> callback) {
		for (size_t i = 0, child_count = ChildCount(); i < child_count; i++) {
			ReplaceChild(callback, i);
		}
	}
	//! Enumerate over all children of this node, invoking the callback for each child.
	void EnumerateChildren(std::function<void(ParsedExpression *expression)> callback) const {
		for (size_t i = 0, child_count = ChildCount(); i < child_count; i++) {
			callback(GetChild(i));
		}
	}

	//! Returns the amount of children of a node
	virtual size_t ChildCount() const;
	//! Returns the i'th child of the expression, or throws an exception if it is out of range
	virtual ParsedExpression *GetChild(size_t index) const;
	//! Replaces the i'th child of this expression. This calls the callback() function with as input the i'th child, and
	//! the return value will be the new child of the expression. [NOTE: inside the callback, the current Expression can
	//! be corrupted as the children are moved away leaving the expression with NULL children where non-NULL children
	//! are expected!]
	virtual void ReplaceChild(std::function<unique_ptr<ParsedExpression>(unique_ptr<ParsedExpression> expression)> callback,
	                          size_t index);

	static bool Equals(ParsedExpression *left, ParsedExpression *right);

	//! Type of the expression
	ExpressionType type;
	//! The expression class of the node
	ExpressionClass expression_class;
	//! The alias of the expression, used in the SELECT clause
	string alias;

	virtual string ToString() const = 0;
	void Print();

protected:
	//! Copy base Expression properties from another expression to this one,
	//! used in Copy method
	void CopyProperties(ParsedExpression &other) {
		type = other.type;
		alias = other.alias;
	}
};

struct ExpressionHashFunction {
	size_t operator()(const ParsedExpression *const &expr) const {
		return (size_t)expr->Hash();
	}
};

struct ExpressionEquality {
	bool operator()(const ParsedExpression *const &a, const ParsedExpression *const &b) const {
		return a->Equals(b);
	}
};

template <typename T>
using expression_map_t = unordered_map<ParsedExpression *, T, ExpressionHashFunction, ExpressionEquality>;

} // namespace duckdb
