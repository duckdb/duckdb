#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/positional_reference_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/expression_binder/where_binder.hpp"

namespace duckdb {

string GetSQLValueFunctionName(const string &column_name) {
	auto lcase = StringUtil::Lower(column_name);
	if (lcase == "current_catalog") {
		return "current_catalog";
	} else if (lcase == "current_date") {
		return "current_date";
	} else if (lcase == "current_schema") {
		return "current_schema";
	} else if (lcase == "current_role") {
		return "current_role";
	} else if (lcase == "current_time") {
		return "get_current_time";
	} else if (lcase == "current_timestamp") {
		return "get_current_timestamp";
	} else if (lcase == "current_user") {
		return "current_user";
	} else if (lcase == "localtime") {
		return "current_localtime";
	} else if (lcase == "localtimestamp") {
		return "current_localtimestamp";
	} else if (lcase == "session_user") {
		return "session_user";
	} else if (lcase == "user") {
		return "user";
	}
	return string();
}

unique_ptr<ParsedExpression> ExpressionBinder::GetSQLValueFunction(const string &column_name) {
	auto value_function = GetSQLValueFunctionName(column_name);
	if (value_function.empty()) {
		return nullptr;
	}

	vector<unique_ptr<ParsedExpression>> children;
	return make_uniq<FunctionExpression>(value_function, std::move(children));
}

unique_ptr<ParsedExpression> ExpressionBinder::QualifyColumnName(const string &column_name, ErrorData &error) {
	auto using_binding = binder.bind_context.GetUsingBinding(column_name);
	if (using_binding) {
		// we are referencing a USING column
		// check if we can refer to one of the base columns directly
		unique_ptr<Expression> expression;
		if (using_binding->primary_binding.IsSet()) {
			// we can! just assign the table name and re-bind
			return binder.bind_context.CreateColumnReference(using_binding->primary_binding, column_name);
		} else {
			// we cannot! we need to bind this as COALESCE between all the relevant columns
			auto coalesce = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_COALESCE);
			coalesce->children.reserve(using_binding->bindings.size());
			for (auto &entry : using_binding->bindings) {
				coalesce->children.push_back(make_uniq<ColumnRefExpression>(column_name, entry));
			}
			return std::move(coalesce);
		}
	}

	// try binding as a lambda parameter
	auto lambda_ref = LambdaRefExpression::FindMatchingBinding(lambda_bindings, column_name);
	if (lambda_ref) {
		return lambda_ref;
	}

	// find a table binding that contains this column name
	auto table_binding = binder.bind_context.GetMatchingBinding(column_name);

	// throw an error if a macro parameter name conflicts with a column name
	auto is_macro_column = false;
	if (binder.macro_binding && binder.macro_binding->HasMatchingBinding(column_name)) {
		is_macro_column = true;
		if (table_binding) {
			throw BinderException("Conflicting column names for column " + column_name + "!");
		}
	}

	// bind as a macro column
	if (is_macro_column) {
		return binder.bind_context.CreateColumnReference(binder.macro_binding->alias, column_name);
	}

	// bind as a regular column
	if (table_binding) {
		return binder.bind_context.CreateColumnReference(table_binding->alias, column_name);
	}

	// it's not, find candidates and error
	auto similar_bindings = binder.bind_context.GetSimilarBindings(column_name);
	error = ErrorData(BinderException::ColumnNotFound(column_name, similar_bindings));
	return nullptr;
}

void ExpressionBinder::QualifyColumnNames(unique_ptr<ParsedExpression> &expr,
                                          vector<unordered_set<string>> &lambda_params,
                                          const bool within_function_expression) {

	bool next_within_function_expression = false;
	switch (expr->type) {
	case ExpressionType::COLUMN_REF: {
		auto &col_ref = expr->Cast<ColumnRefExpression>();

		// don't qualify lambda parameters
		if (LambdaExpression::IsLambdaParameter(lambda_params, col_ref.GetName())) {
			return;
		}

		ErrorData error;
		auto new_expr = QualifyColumnName(col_ref, error);

		if (new_expr) {
			if (!expr->alias.empty()) {
				// Pre-existing aliases are added to the qualified column reference
				new_expr->alias = expr->alias;
			} else if (within_function_expression) {
				// Qualifying the column reference may add an alias, but this needs to be removed within function
				// expressions, because the alias here means a named parameter instead of a positional parameter
				new_expr->alias = "";
			}

			// replace the expression with the qualified column reference
			new_expr->query_location = col_ref.query_location;
			expr = std::move(new_expr);
		}
		return;
	}
	case ExpressionType::POSITIONAL_REFERENCE: {
		auto &ref = expr->Cast<PositionalReferenceExpression>();
		if (ref.alias.empty()) {
			string table_name, column_name;
			auto error = binder.bind_context.BindColumn(ref, table_name, column_name);
			if (error.empty()) {
				ref.alias = column_name;
			}
		}
		break;
	}
	case ExpressionType::FUNCTION: {
		// Special-handling for lambdas, which are inside function expressions.
		auto &function = expr->Cast<FunctionExpression>();
		if (function.IsLambdaFunction()) {
			return QualifyColumnNamesInLambda(function, lambda_params);
		}

		next_within_function_expression = true;
		break;
	}
	default: // fall through
		break;
	}

	// recurse on the child expressions
	ParsedExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<ParsedExpression> &child) {
		QualifyColumnNames(child, lambda_params, next_within_function_expression);
	});
}

void ExpressionBinder::QualifyColumnNamesInLambda(FunctionExpression &function,
                                                  vector<unordered_set<string>> &lambda_params) {

	for (auto &child : function.children) {
		if (child->expression_class != ExpressionClass::LAMBDA) {
			// not a lambda expression
			QualifyColumnNames(child, lambda_params, true);
			continue;
		}

		// special-handling for LHS lambda parameters
		// we do not qualify them, and we add them to the lambda_params vector
		auto &lambda_expr = child->Cast<LambdaExpression>();
		string error_message;
		auto column_ref_expressions = lambda_expr.ExtractColumnRefExpressions(error_message);

		if (!error_message.empty()) {
			// possibly a JSON function, qualify both LHS and RHS
			QualifyColumnNames(lambda_expr.lhs, lambda_params, true);
			QualifyColumnNames(lambda_expr.expr, lambda_params, true);
			continue;
		}

		// push this level
		lambda_params.emplace_back();

		// push the lambda parameter names
		for (const auto &column_ref_expr : column_ref_expressions) {
			const auto &column_ref = column_ref_expr.get().Cast<ColumnRefExpression>();
			lambda_params.back().emplace(column_ref.GetName());
		}

		// only qualify in RHS
		QualifyColumnNames(lambda_expr.expr, lambda_params, true);

		// pop this level
		lambda_params.pop_back();
	}
}

void ExpressionBinder::QualifyColumnNames(Binder &binder, unique_ptr<ParsedExpression> &expr) {
	WhereBinder where_binder(binder, binder.context);
	vector<unordered_set<string>> lambda_params;
	where_binder.QualifyColumnNames(expr, lambda_params);
}

void ExpressionBinder::QualifyColumnNames(ExpressionBinder &expression_binder, unique_ptr<ParsedExpression> &expr) {
	vector<unordered_set<string>> lambda_params;
	expression_binder.QualifyColumnNames(expr, lambda_params);
}

unique_ptr<ParsedExpression> ExpressionBinder::CreateStructExtract(unique_ptr<ParsedExpression> base,
                                                                   const string &field_name) {

	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(std::move(base));
	children.push_back(make_uniq_base<ParsedExpression, ConstantExpression>(Value(field_name)));
	auto extract_fun = make_uniq<OperatorExpression>(ExpressionType::STRUCT_EXTRACT, std::move(children));
	return std::move(extract_fun);
}

unique_ptr<ParsedExpression> ExpressionBinder::CreateStructPack(ColumnRefExpression &col_ref) {
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
	vector<unique_ptr<ParsedExpression>> child_expressions;
	child_expressions.reserve(binding->names.size());
	for (const auto &column_name : binding->names) {
		child_expressions.push_back(binder.bind_context.CreateColumnReference(
		    binding->alias, column_name, ColumnBindType::DO_NOT_EXPAND_GENERATED_COLUMNS));
	}
	return make_uniq<FunctionExpression>("struct_pack", std::move(child_expressions));
}

unique_ptr<ParsedExpression> ExpressionBinder::QualifyColumnNameWithManyDotsInternal(ColumnRefExpression &col_ref,
                                                                                     ErrorData &error,
                                                                                     idx_t &struct_extract_start) {
	// two or more dots (i.e. "part1.part2.part3.part4...")
	// -> part1 is a catalog, part2 is a schema, part3 is a table, part4 is a column name, part 5 and beyond are
	// struct fields
	// -> part1 is a catalog, part2 is a table, part3 is a column name, part4 and beyond are struct fields
	// -> part1 is a schema, part2 is a table, part3 is a column name, part4 and beyond are struct fields
	// -> part1 is a table, part2 is a column name, part3 and beyond are struct fields
	// -> part1 is a column, part2 and beyond are struct fields

	// we always prefer the most top-level view
	// i.e. in case of multiple resolution options, we resolve in order:
	// -> 1. resolve "part1" as a catalog
	// -> 2. resolve "part1" as a schema
	// -> 3. resolve "part1" as a table
	// -> 4. resolve "part1" as a column

	// first check if part1 is a catalog
	optional_ptr<Binding> binding;
	if (col_ref.column_names.size() > 3) {
		binding = binder.GetMatchingBinding(col_ref.column_names[0], col_ref.column_names[1], col_ref.column_names[2],
		                                    col_ref.column_names[3], error);
		if (binding) {
			// part1 is a catalog - the column reference is "catalog.schema.table.column"
			struct_extract_start = 4;
			return binder.bind_context.CreateColumnReference(binding->alias, col_ref.column_names[3]);
		}
	}
	binding = binder.GetMatchingBinding(col_ref.column_names[0], INVALID_SCHEMA, col_ref.column_names[1],
	                                    col_ref.column_names[2], error);
	if (binding) {
		// part1 is a catalog - the column reference is "catalog.table.column"
		struct_extract_start = 3;
		return binder.bind_context.CreateColumnReference(binding->alias, col_ref.column_names[2]);
	}
	binding =
	    binder.GetMatchingBinding(col_ref.column_names[0], col_ref.column_names[1], col_ref.column_names[2], error);
	if (binding) {
		// part1 is a schema - the column reference is "schema.table.column"
		// any additional fields are turned into struct_extract calls
		struct_extract_start = 3;
		return binder.bind_context.CreateColumnReference(binding->alias, col_ref.column_names[2]);
	}
	binding = binder.GetMatchingBinding(col_ref.column_names[0], col_ref.column_names[1], error);
	if (binding) {
		// part1 is a table
		// the column reference is "table.column"
		// any additional fields are turned into struct_extract calls
		struct_extract_start = 2;
		return binder.bind_context.CreateColumnReference(binding->alias, col_ref.column_names[1]);
	}
	// part1 could be a column
	ErrorData col_error;
	auto result_expr = QualifyColumnName(col_ref.column_names[0], col_error);
	if (result_expr) {
		// it is! add the struct extract calls
		struct_extract_start = 1;
		return result_expr;
	}
	return CreateStructPack(col_ref);
}
unique_ptr<ParsedExpression> ExpressionBinder::QualifyColumnNameWithManyDots(ColumnRefExpression &col_ref,
                                                                             ErrorData &error) {
	idx_t struct_extract_start = col_ref.column_names.size();
	auto result_expr = QualifyColumnNameWithManyDotsInternal(col_ref, error, struct_extract_start);
	if (!result_expr) {
		return nullptr;
	}

	// create a struct extract with all remaining column names
	for (idx_t i = struct_extract_start; i < col_ref.column_names.size(); i++) {
		result_expr = CreateStructExtract(std::move(result_expr), col_ref.column_names[i]);
	}

	return result_expr;
}

unique_ptr<ParsedExpression> ExpressionBinder::QualifyColumnName(ColumnRefExpression &col_ref, ErrorData &error) {

	// try binding as a lambda parameter
	if (!col_ref.IsQualified()) {
		auto lambda_ref = LambdaRefExpression::FindMatchingBinding(lambda_bindings, col_ref.GetName());
		if (lambda_ref) {
			return lambda_ref;
		}
	}

	idx_t column_parts = col_ref.column_names.size();

	// column names can have an arbitrary amount of dots
	// here is how the resolution works:
	if (column_parts == 1) {
		// no dots (i.e. "part1")
		// -> part1 refers to a column
		// check if we can qualify the column name with the table name
		auto qualified_col_ref = QualifyColumnName(col_ref.GetColumnName(), error);
		if (qualified_col_ref) {
			// we could: return it
			return qualified_col_ref;
		}
		// we could not! Try creating an implicit struct_pack
		return CreateStructPack(col_ref);
	}

	if (column_parts == 2) {
		// one dot (i.e. "part1.part2")
		// EITHER:
		// -> part1 is a table, part2 is a column
		// -> part1 is a column, part2 is a property of that column (i.e. struct_extract)

		// first check if part1 is a table, and part2 is a standard column name
		auto binding = binder.GetMatchingBinding(col_ref.column_names[0], col_ref.column_names[1], error);
		if (binding) {
			// it is! return the column reference directly
			return binder.bind_context.CreateColumnReference(binding->alias, col_ref.GetColumnName());
		}

		// otherwise check if we can turn this into a struct extract
		ErrorData other_error;
		auto qualified_col_ref = QualifyColumnName(col_ref.column_names[0], other_error);
		if (qualified_col_ref) {
			// we could: create a struct extract
			return CreateStructExtract(std::move(qualified_col_ref), col_ref.column_names[1]);
		}
		// we could not! Try creating an implicit struct_pack
		return CreateStructPack(col_ref);
	}

	// three or more dots
	return QualifyColumnNameWithManyDots(col_ref, error);
}

BindResult ExpressionBinder::BindExpression(LambdaRefExpression &lambda_ref, idx_t depth) {
	D_ASSERT(lambda_bindings && lambda_ref.lambda_idx < lambda_bindings->size());
	return (*lambda_bindings)[lambda_ref.lambda_idx].Bind(lambda_ref, depth);
}

BindResult ExpressionBinder::BindExpression(ColumnRefExpression &col_ref_p, idx_t depth, bool root_expression) {
	if (binder.GetBindingMode() == BindingMode::EXTRACT_NAMES) {
		return BindResult(make_uniq<BoundConstantExpression>(Value(LogicalType::SQLNULL)));
	}

	ErrorData error;
	auto expr = QualifyColumnName(col_ref_p, error);
	if (!expr) {
		if (!col_ref_p.IsQualified()) {
			// column was not found
			// first try to bind it as an alias
			BindResult alias_result;
			auto found_alias = TryBindAlias(col_ref_p, root_expression, alias_result);
			if (found_alias) {
				return alias_result;
			}

			// column was not found - check if it is a SQL value function
			auto value_function = GetSQLValueFunction(col_ref_p.GetColumnName());
			if (value_function) {
				return BindExpression(value_function, depth);
			}
		}
		error.AddQueryLocation(col_ref_p);
		return BindResult(std::move(error));
	}

	expr->query_location = col_ref_p.query_location;

	// the above QualifyColumName returns a generated expression for a generated
	// column, and struct_extract for a struct, or a lambda reference expression,
	// all of them are not column reference expressions, so we return here
	if (expr->type != ExpressionType::COLUMN_REF) {
		auto alias = expr->alias;
		auto result = BindExpression(expr, depth);
		if (result.expression) {
			result.expression->alias = std::move(alias);
		}
		return result;
	}

	// the above QualifyColumnName returned an individual column reference
	// expression, which we resolve to either a base table or a subquery expression,
	// and if it was a macro parameter, then we let macro_binding bind it to the argument
	BindResult result;
	auto &col_ref = expr->Cast<ColumnRefExpression>();
	D_ASSERT(col_ref.IsQualified());
	auto &table_name = col_ref.GetTableName();

	if (binder.macro_binding && table_name == binder.macro_binding->GetAlias()) {
		result = binder.macro_binding->Bind(col_ref, depth);
	} else {
		result = binder.bind_context.BindColumn(col_ref, depth);
	}

	if (result.HasError()) {
		result.error.AddQueryLocation(col_ref_p);
		return result;
	}

	// we bound the column reference
	BoundColumnReferenceInfo ref;
	ref.name = col_ref.column_names.back();
	ref.query_location = col_ref.query_location;
	bound_columns.push_back(std::move(ref));
	return result;
}

bool ExpressionBinder::QualifyColumnAlias(const ColumnRefExpression &col_ref) {
	// only the BaseSelectBinder will have a valid column alias map,
	// otherwise we return false
	return false;
}
} // namespace duckdb
