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
	virtual void Visit(LambdaExpression &expr) = 0;

protected:
	GeneratedExpressionVisitor() {
	}
};

//===--------------------------------------------------------------------===//
// AliasReplacer
//===--------------------------------------------------------------------===//

class AliasReplacer : public GeneratedExpressionVisitor {
public:
	AliasReplacer(ColumnList &list, unordered_map<idx_t, string> &alias_map) : list(list), alias_map(alias_map) {
	}
	void Visit(ColumnRefExpression &expr) override;
	void Visit(LambdaExpression &expr) override;

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
	void Visit(ColumnRefExpression &expr) final override;
	void Visit(LambdaExpression &expr) final override;

private:
	const string &table_name;
};

//===--------------------------------------------------------------------===//
// ColumnDependencyLister
//===--------------------------------------------------------------------===//

class ColumnDependencyLister : public GeneratedExpressionVisitor {
public:
	ColumnDependencyLister(vector<string> &dependencies, unordered_set<string> &excludes)
	    : dependencies(dependencies), excludes(excludes) {
	}
	void Visit(ColumnRefExpression &expr) final override;
	void Visit(LambdaExpression &expr) final override;

private:
	vector<string> &dependencies;
	unordered_set<string> &excludes;
};

} // namespace duckdb
