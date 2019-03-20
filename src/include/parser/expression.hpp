//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/types/statistics.hpp"

#include <functional>
#include <memory>
#include <stack>
#include <vector>

namespace duckdb {
class SQLNodeVisitor;
class AggregateExpression;

//!  Expression class is a base class that can represent any expression
//!  part of a SQL statement.
/*!
 The Expression class is a base class that can represent any expression
 part of a SQL statement. This is, for example, a column reference in a SELECT
 clause, but also operators, aggregates or filters.

 In the execution engine, an Expression always returns a single Vector
 of the type specified by return_type. It can take an arbitrary amount of
 Vectors as input (but in most cases the amount of input vectors is 0-2).
 */
class Expression {
public:
	//! Create an Expression
	Expression(ExpressionType type) : type(type), stats(*this) {
	}
	//! Create an Expression with the specified return type
	Expression(ExpressionType type, TypeId return_type) : type(type), return_type(return_type), stats(*this) {
	}
	virtual ~Expression() {
	}

	//! Resolves the type for this expression based on its children
	virtual void ResolveType() {
		EnumerateChildren([](Expression *child) { child->ResolveType(); });
	}

	//! Returns true if this Expression is an aggregate or not.
	/*!
	 Examples:

	 (1) SUM(a) + 1 -- True

	 (2) a + 1 -- False
	 */
	virtual bool IsAggregate();

	virtual bool IsWindow();

	//! Returns true if the query contains a subquery
	virtual bool HasSubquery();

	// returns true if expression does not contain a group ref or col ref or
	// parameter
	virtual bool IsScalar();

	virtual bool HasParameter();

	//! Returns whether or not the expression is foldable into a constant
	bool IsFoldable() {
		return IsScalar() && !IsAggregate() && !IsWindow() && !HasParameter();
	}

	//! Returns the type of the expression
	ExpressionType GetExpressionType() {
		return type;
	}

	//! Creates a hash value of this expression. It is important that if two expressions are identical (i.e.
	//! Expression::Equals() returns true), that their hash value is identical as well.
	virtual uint64_t Hash() const;
	//! Returns true if this expression is equal to another expression
	virtual bool Equals(const Expression *other) const;

	bool operator==(const Expression &rhs) {
		return this->Equals(&rhs);
	}

	virtual string GetName() const {
		return !alias.empty() ? alias : "Unknown";
	}
	virtual ExpressionClass GetExpressionClass() = 0;

	//! Create a copy of this expression
	virtual unique_ptr<Expression> Copy() const = 0;

	//! Serializes an Expression to a stand-alone binary blob
	virtual void Serialize(Serializer &serializer);
	//! Deserializes a blob back into an Expression [CAN THROW:
	//! SerializationException]
	static unique_ptr<Expression> Deserialize(Deserializer &source);

	//! Enumerate over all children of this node, invoking the callback for each child. This method allows replacing the
	//! children using the return value. [NOTE: inside the callback, the current Expression can be corrupted as the
	//! children are moved away leaving the expression with NULL children where non-NULL children are expected!]
	void EnumerateChildren(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback) {
		for (size_t i = 0, child_count = ChildCount(); i < child_count; i++) {
			ReplaceChild(callback, i);
		}
	}
	//! Enumerate over all children of this node, invoking the callback for each child.
	void EnumerateChildren(std::function<void(Expression *expression)> callback) const {
		for (size_t i = 0, child_count = ChildCount(); i < child_count; i++) {
			callback(GetChild(i));
		}
	}
	//! Returns the amount of children of a node
	virtual size_t ChildCount() const;
	//! Returns the i'th child of the expression, or throws an exception if it is out of range
	virtual Expression *GetChild(size_t index) const;
	//! Replaces the i'th child of this expression. This calls the callback() function with as input the i'th child, and
	//! the return value will be the new child of the expression. [NOTE: inside the callback, the current Expression can
	//! be corrupted as the children are moved away leaving the expression with NULL children where non-NULL children
	//! are expected!]
	virtual void ReplaceChild(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback,
	                          size_t index);

	//! Clears the statistics of this expression and all child expressions
	void ClearStatistics() {
		stats.has_stats = false;
		EnumerateChildren([](Expression *child) { child->ClearStatistics(); });
	}

	static bool Equals(Expression *left, Expression *right) {
		if (left == right) {
			// if pointers are equivalent, they are equivalent
			return true;
		}
		if (!left || !right) {
			// otherwise if one of them is nullptr, they are not equivalent
			// because the other one cannot be nullptr then
			return false;
		}
		// otherwise we use the normal equality
		return left->Equals(right);
	}

	//! Type of the expression
	ExpressionType type;
	//! Return type of the expression. This must be known in the execution
	//! engine
	TypeId return_type = TypeId::INVALID;

	//! The statistics of the current expression in the plan
	ExpressionStatistics stats;

	//! The alias of the expression, used in the SELECT clause (e.g. SELECT x +
	//! 1 AS f)
	string alias;

	virtual string ToString() const = 0;
	void Print();

protected:
	//! Copy base Expression properties from another expression to this one,
	//! used in Copy method
	void CopyProperties(const Expression &other) {
		type = other.type;
		return_type = other.return_type;
		stats = other.stats;
		alias = other.alias;
	}
};

struct ExpressionHashFunction {
	size_t operator()(const Expression *const &expr) const {
		return (size_t)expr->Hash();
	}
};

struct ExpressionEquality {
	bool operator()(const Expression *const &a, const Expression *const &b) const {
		return a->Equals(b);
	}
};

template <typename T>
using expression_map_t = unordered_map<Expression *, T, ExpressionHashFunction, ExpressionEquality>;

} // namespace duckdb
