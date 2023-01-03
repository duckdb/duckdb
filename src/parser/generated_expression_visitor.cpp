#include "duckdb/parser/generated_expression_visitor.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Base visitor
//===--------------------------------------------------------------------===//

void GeneratedExpressionVisitor::VisitExpression(ParsedExpression &expr) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		Visit((ColumnRefExpression &)expr);
	} else if (expr.type == ExpressionType::LAMBDA) {
		Visit((LambdaExpression &)expr);
	} else {
		ParsedExpressionIterator::EnumerateChildren(
		    expr, [&](const ParsedExpression &child) { this->VisitExpression((ParsedExpression &)child); });
	}
}

//===--------------------------------------------------------------------===//
// AliasReplacer
//===--------------------------------------------------------------------===//

void AliasReplacer::Visit(ColumnRefExpression &expr) {
	D_ASSERT(!expr.IsQualified());
	auto &col_names = expr.column_names;
	D_ASSERT(col_names.size() == 1);
	auto idx_entry = list.GetColumnIndex(col_names[0]);
	auto &alias = alias_map.at(idx_entry.index);
	col_names = {alias};
}

void AliasReplacer::Visit(LambdaExpression &expr) {
	// VisitExpression(*expr.lhs);
	// FIXME: should we skip the expr of the lambda?
}

//===--------------------------------------------------------------------===//
// ColumnQualifier
//===--------------------------------------------------------------------===//

void ColumnQualifier::Visit(ColumnRefExpression &expr) {
	auto &colref = (ColumnRefExpression &)expr;
	D_ASSERT(!colref.IsQualified());
	auto &col_names = colref.column_names;
	col_names.insert(col_names.begin(), table_name);
}

void ColumnQualifier::Visit(LambdaExpression &expr) {
}

//===--------------------------------------------------------------------===//
// ColumnDependencyLister
//===--------------------------------------------------------------------===//

static void AddExcludedColumns(ParsedExpression &expr, unordered_set<string> &excludes) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto columnref = (ColumnRefExpression &)expr;
		auto &name = columnref.GetColumnName();
		excludes.insert(name);
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { AddExcludedColumns((ParsedExpression &)child, excludes); });
}

void ColumnDependencyLister::Visit(ColumnRefExpression &expr) {
	auto columnref = (ColumnRefExpression &)expr;
	auto &name = columnref.GetColumnName();
	if (excludes.count(name)) {
		return;
	}
	dependencies.push_back(name);
}

void ColumnDependencyLister::Visit(LambdaExpression &expr) {
	// Skip it so we don't register lambda column references as dependencies
	auto &lambda_expr = (LambdaExpression &)expr;
	AddExcludedColumns(*lambda_expr.lhs, excludes);
	VisitExpression(*lambda_expr.expr);
}

} // namespace duckdb
