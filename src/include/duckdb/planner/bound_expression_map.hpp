//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/bound_expression_map.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

//! BoundExpressionMap maps parsed expression nodes (by identity) to their bound expressions.
//! The binder uses it to prevent re-binding of already bound parts when incrementally re-binding
//! the same parsed tree against outer scopes (see ExpressionBinder::BindCorrelatedColumns).
//! Entries are scoped: every full bind cycle opens a BoundExpressionScope, and entries that are
//! not consumed by the end of the cycle are erased so no entry can outlive its parsed node.
class BoundExpressionMap {
public:
	//! Insert the bound expression for a parsed node, overwriting any existing entry.
	//! Requires an open scope.
	void Insert(const ParsedExpression &node, unique_ptr<Expression> expr);
	//! Whether this node has a bound expression
	bool IsBound(const ParsedExpression &node) const;
	//! Return the bound expression of a node
	Expression &Get(const ParsedExpression &node) const;
	//! Return a mutable slot holding the bound expression of a node
	unique_ptr<Expression> &GetMutable(const ParsedExpression &node);
	//! Remove the entry for a node and return its bound expression
	unique_ptr<Expression> Consume(const ParsedExpression &node);
	//! Whether the node itself or any descendant has a bound expression
	bool HasBoundDescendant(const ParsedExpression &node) const;
	//! Erase the entries of the node and all of its descendants, if any
	void EraseSubtree(const ParsedExpression &node);
	bool Empty() const {
		return entries.empty();
	}

private:
	struct MapEntry {
		unique_ptr<Expression> expression;
		//! The scope the entry was inserted in
		idx_t scope_index = 0;
#ifdef DEBUG
		//! Identity stamp of the parsed node, to detect stale entries under pointer reuse
		ExpressionClass expr_class;
		ExpressionType expr_type;
#endif
	};

	const MapEntry &GetEntry(const ParsedExpression &node) const;
	void VerifyEntry(const ParsedExpression &node) const;

private:
	reference_map_t<const ParsedExpression, MapEntry> entries;
	//! One frame per open scope, recording the nodes inserted while it was active
	vector<vector<reference<const ParsedExpression>>> scope_stack;

	friend class BoundExpressionScope;
};

//! RAII scope for BoundExpressionMap entries: opened by every full bind cycle, erases the
//! entries inserted within it that were not consumed - also when unwinding on a binding error
class BoundExpressionScope {
public:
	explicit BoundExpressionScope(BoundExpressionMap &bound_expressions);
	~BoundExpressionScope();

private:
	BoundExpressionMap &bound_expressions;
};

} // namespace duckdb
