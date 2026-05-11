#include "duckdb/planner/column_qualifier.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/positional_reference_expression.hpp"

namespace duckdb {

ColumnQualifier::ColumnQualifier(Binder &binder_p) : binder(binder_p) {}

unique_ptr<ParsedExpression> ColumnQualifier::CreateStructExtract(unique_ptr<ParsedExpression> base,
                                                                   const string &field_name) {
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(std::move(base));
	children.push_back(make_uniq_base<ParsedExpression, ConstantExpression>(Value(field_name)));
	auto extract_fun = make_uniq<OperatorExpression>(ExpressionType::STRUCT_EXTRACT, std::move(children));
	return std::move(extract_fun);
}

unique_ptr<ParsedExpression> ColumnQualifier::CreateStructPack(ColumnRefExpression &col_ref) {
	if (col_ref.column_names.size() > 3) {
		return nullptr;
	}
	D_ASSERT(!col_ref.column_names.empty());

	// get a matching binding
	ErrorData error;
	optional_ptr<Binding> binding;
	switch (col_ref.column_names.size()) {
	case 1: {
		// single entry - this must be the table name
		BindingAlias alias(col_ref.column_names[0]);
		binding = binder.bind_context.GetBinding(alias, error);
		break;
	}
	case 2: {
		// two entries - this can either be "catalog.table" or "schema.table" - try both
		BindingAlias alias(col_ref.column_names[0], col_ref.column_names[1]);
		binding = binder.bind_context.GetBinding(alias, error);
		if (!binding) {
			alias = BindingAlias(col_ref.column_names[0], INVALID_SCHEMA, col_ref.column_names[1]);
			binding = binder.bind_context.GetBinding(alias, error);
		}
		break;
	}
	case 3: {
		// three entries - this must be "catalog.schema.table"
		BindingAlias alias(col_ref.column_names[0], col_ref.column_names[1], col_ref.column_names[2]);
		binding = binder.bind_context.GetBinding(alias, error);
		break;
	}
	default:
		throw InternalException("Expected 1, 2 or 3 column names for CreateStructPack");
	}
	if (!binding) {
		return nullptr;
	}

	// We found the table, now create the struct_pack expression
	auto &column_names = binding->GetColumnNames();
	vector<unique_ptr<ParsedExpression>> child_expressions;
	child_expressions.reserve(column_names.size());
	for (const auto &column_name : column_names) {
		child_expressions.push_back(binder.bind_context.CreateColumnReference(
		    binding->GetBindingAlias(), column_name, ColumnBindType::DO_NOT_EXPAND_GENERATED_COLUMNS));
	}
	return make_uniq<FunctionExpression>("struct_pack", std::move(child_expressions));
}

}
