#include "duckdb/parser/generated_expression_visitor.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Base visitor
//===--------------------------------------------------------------------===//

void GeneratedExpressionVisitor::ExtractLambdaParameters(ParsedExpression &expr) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &column_ref = (ColumnRefExpression &)expr;
		D_ASSERT(!column_ref.IsQualified());
		auto &col_names = column_ref.column_names;
		D_ASSERT(col_names.size() == 1);
		lambda_parameters.insert(col_names[0]);
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { this->ExtractLambdaParameters((ParsedExpression &)child); });
}

void GeneratedExpressionVisitor::VisitExpression(ParsedExpression &expr) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		Visit((ColumnRefExpression &)expr);
	} else if (expr.type == ExpressionType::LAMBDA) {
		auto &lambda_expr = (LambdaExpression &)expr;
		ExtractLambdaParameters(*lambda_expr.lhs);
		VisitExpression(*lambda_expr.expr);
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
	if (lambda_parameters.count(col_names[0])) {
		return;
	}
	auto idx_entry = list.GetColumnIndex(col_names[0]);
	auto &alias = alias_map.at(idx_entry.index);
	col_names = {alias};
}

//===--------------------------------------------------------------------===//
// ColumnQualifier
//===--------------------------------------------------------------------===//

void ColumnQualifier::Visit(ColumnRefExpression &expr) {
	auto &colref = (ColumnRefExpression &)expr;
	D_ASSERT(!colref.IsQualified());
	auto &col_names = colref.column_names;
	if (lambda_parameters.count(col_names[0])) {
		return;
	}
	col_names.insert(col_names.begin(), table_name);
}

//===--------------------------------------------------------------------===//
// ColumnDependencyLister
//===--------------------------------------------------------------------===//

void ColumnDependencyLister::Visit(ColumnRefExpression &expr) {
	auto columnref = (ColumnRefExpression &)expr;
	auto &name = columnref.GetColumnName();
	if (lambda_parameters.count(name)) {
		return;
	}
	dependencies.push_back(name);
}

} // namespace duckdb
