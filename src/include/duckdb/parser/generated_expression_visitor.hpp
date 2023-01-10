#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Base visitor
//===--------------------------------------------------------------------===//

class GeneratedExpressionVisitor {
public:
	void VisitExpression(ParsedExpression &expr);
	virtual void Visit(ColumnRefExpression &expr) = 0;

protected:
	GeneratedExpressionVisitor() {
	}

protected:
	unordered_set<string> lambda_parameters;

private:
	// Walk the lambda's lhs expression, saving all column references encountered
	void ExtractLambdaParameters(ParsedExpression &expr);
};

//===--------------------------------------------------------------------===//
// AliasReplacer
//===--------------------------------------------------------------------===//

class AliasReplacer : public GeneratedExpressionVisitor {
public:
	AliasReplacer(ColumnList &list, unordered_map<idx_t, string> &alias_map) : list(list), alias_map(alias_map) {
	}
	//! Replace column references with their aliases
	void Visit(ColumnRefExpression &expr) override;

private:
	const ColumnList &list;
	const unordered_map<idx_t, string> &alias_map;
};

//===--------------------------------------------------------------------===//
// ColumnQualifier
//===--------------------------------------------------------------------===//

class ColumnQualifier : public GeneratedExpressionVisitor {
public:
	ColumnQualifier(const string &table_name) : table_name(table_name) {
	}
	// Qualify all column references with the table_name
	void Visit(ColumnRefExpression &expr) final override;

private:
	const string &table_name;
};

//===--------------------------------------------------------------------===//
// ColumnDependencyLister
//===--------------------------------------------------------------------===//

class ColumnDependencyLister : public GeneratedExpressionVisitor {
public:
	ColumnDependencyLister(vector<string> &dependencies) : dependencies(dependencies) {
	}
	//! Register all column references found in the expression
	void Visit(ColumnRefExpression &expr) final override;

private:
	vector<string> &dependencies;
};

} // namespace duckdb
