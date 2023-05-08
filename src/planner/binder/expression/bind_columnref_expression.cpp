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
#include "duckdb/planner/expression/bound_lambdaref_expression.hpp"
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

unique_ptr<ParsedExpression> ExpressionBinder::QualifyColumnName(const string &column_name, string &error_message) {
	auto using_binding = binder.bind_context.GetUsingBinding(column_name);
	if (using_binding) {
		// we are referencing a USING column
		// check if we can refer to one of the base columns directly
		unique_ptr<Expression> expression;
		if (!using_binding->primary_binding.empty()) {
			// we can! just assign the table name and re-bind
			return binder.bind_context.CreateColumnReference(using_binding->primary_binding, column_name);
		} else {
			// // we cannot! we need to bind this as a coalesce between all the relevant columns
			auto coalesce = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_COALESCE);
			coalesce->children.reserve(using_binding->bindings.size());
			for (auto &entry : using_binding->bindings) {
				coalesce->children.push_back(make_uniq<ColumnRefExpression>(column_name, entry));
			}
			return std::move(coalesce);
		}
	}

	// find a binding that contains this
	string table_name = binder.bind_context.GetMatchingBinding(column_name);

	// throw an error if a macro conflicts with a column name
	auto is_macro_column = false;
	if (binder.macro_binding != nullptr && binder.macro_binding->HasMatchingBinding(column_name)) {
		is_macro_column = true;
		if (!table_name.empty()) {
			throw BinderException("Conflicting column names for column " + column_name + "!");
		}
	}

	if (lambda_bindings) {
		for (idx_t i = 0; i < lambda_bindings->size(); i++) {
			if ((*lambda_bindings)[i].HasMatchingBinding(column_name)) {

				// throw an error if a lambda conflicts with a column name or a macro
				if (!table_name.empty() || is_macro_column) {
					throw BinderException("Conflicting column names for column " + column_name + "!");
				}

				D_ASSERT(!(*lambda_bindings)[i].alias.empty());
				return make_uniq<ColumnRefExpression>(column_name, (*lambda_bindings)[i].alias);
			}
		}
	}

	if (is_macro_column) {
		D_ASSERT(!binder.macro_binding->alias.empty());
		return make_uniq<ColumnRefExpression>(column_name, binder.macro_binding->alias);
	}
	// see if it's a column
	if (table_name.empty()) {
		// column was not found - check if it is a SQL value function
		auto value_function = GetSQLValueFunction(column_name);
		if (value_function) {
			return value_function;
		}
		// it's not, find candidates and error
		auto similar_bindings = binder.bind_context.GetSimilarBindings(column_name);
		string candidate_str = StringUtil::CandidatesMessage(similar_bindings, "Candidate bindings");
		error_message =
		    StringUtil::Format("Referenced column \"%s\" not found in FROM clause!%s", column_name, candidate_str);
		return nullptr;
	}
	return binder.bind_context.CreateColumnReference(table_name, column_name);
}

void ExpressionBinder::QualifyColumnNames(unique_ptr<ParsedExpression> &expr) {
	switch (expr->type) {
	case ExpressionType::COLUMN_REF: {
		auto &colref = expr->Cast<ColumnRefExpression>();
		string error_message;
		auto new_expr = QualifyColumnName(colref, error_message);
		if (new_expr) {
			if (!expr->alias.empty()) {
				new_expr->alias = expr->alias;
			}
			new_expr->query_location = colref.query_location;
			expr = std::move(new_expr);
		}
		break;
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
	default:
		break;
	}
	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child) { QualifyColumnNames(child); });
}

void ExpressionBinder::QualifyColumnNames(Binder &binder, unique_ptr<ParsedExpression> &expr) {
	WhereBinder where_binder(binder, binder.context);
	where_binder.QualifyColumnNames(expr);
}

unique_ptr<ParsedExpression> ExpressionBinder::CreateStructExtract(unique_ptr<ParsedExpression> base,
                                                                   string field_name) {

	// we need to transform the struct extract if it is inside a lambda expression
	// because we cannot bind to an existing table, so we remove the dummy table also
	if (lambda_bindings && base->type == ExpressionType::COLUMN_REF) {
		auto &lambda_column_ref = base->Cast<ColumnRefExpression>();
		D_ASSERT(!lambda_column_ref.column_names.empty());

		if (lambda_column_ref.column_names[0].find(DummyBinding::DUMMY_NAME) != string::npos) {
			D_ASSERT(lambda_column_ref.column_names.size() == 2);
			auto lambda_param_name = lambda_column_ref.column_names.back();
			lambda_column_ref.column_names.clear();
			lambda_column_ref.column_names.push_back(lambda_param_name);
		}
	}

	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(std::move(base));
	children.push_back(make_uniq_base<ParsedExpression, ConstantExpression>(Value(std::move(field_name))));
	auto extract_fun = make_uniq<OperatorExpression>(ExpressionType::STRUCT_EXTRACT, std::move(children));
	return std::move(extract_fun);
}

unique_ptr<ParsedExpression> ExpressionBinder::CreateStructPack(ColumnRefExpression &colref) {
	D_ASSERT(colref.column_names.size() <= 3);
	string error_message;
	auto &table_name = colref.column_names.back();
	auto binding = binder.bind_context.GetBinding(table_name, error_message);
	if (!binding) {
		return nullptr;
	}
	if (colref.column_names.size() >= 2) {
		// "schema_name.table_name"
		auto catalog_entry = binding->GetStandardEntry();
		if (!catalog_entry) {
			return nullptr;
		}
		if (catalog_entry->name != table_name) {
			return nullptr;
		}
		if (colref.column_names.size() == 2) {
			auto &qualifier = colref.column_names[0];
			if (catalog_entry->catalog.GetName() != qualifier && catalog_entry->schema.name != qualifier) {
				return nullptr;
			}
		} else if (colref.column_names.size() == 3) {
			auto &catalog_name = colref.column_names[0];
			auto &schema_name = colref.column_names[1];
			if (catalog_entry->catalog.GetName() != catalog_name || catalog_entry->schema.name != schema_name) {
				return nullptr;
			}
		} else {
			throw InternalException("Expected 2 or 3 column names for CreateStructPack");
		}
	}
	// We found the table, now create the struct_pack expression
	vector<unique_ptr<ParsedExpression>> child_expressions;
	child_expressions.reserve(binding->names.size());
	for (const auto &column_name : binding->names) {
		child_expressions.push_back(make_uniq<ColumnRefExpression>(column_name, table_name));
	}
	return make_uniq<FunctionExpression>("struct_pack", std::move(child_expressions));
}

unique_ptr<ParsedExpression> ExpressionBinder::QualifyColumnName(ColumnRefExpression &colref, string &error_message) {
	idx_t column_parts = colref.column_names.size();
	// column names can have an arbitrary amount of dots
	// here is how the resolution works:
	if (column_parts == 1) {
		// no dots (i.e. "part1")
		// -> part1 refers to a column
		// check if we can qualify the column name with the table name
		auto qualified_colref = QualifyColumnName(colref.GetColumnName(), error_message);
		if (qualified_colref) {
			// we could: return it
			return qualified_colref;
		}
		// we could not! Try creating an implicit struct_pack
		return CreateStructPack(colref);
	} else if (column_parts == 2) {
		// one dot (i.e. "part1.part2")
		// EITHER:
		// -> part1 is a table, part2 is a column
		// -> part1 is a column, part2 is a property of that column (i.e. struct_extract)

		// first check if part1 is a table, and part2 is a standard column
		if (binder.HasMatchingBinding(colref.column_names[0], colref.column_names[1], error_message)) {
			// it is! return the colref directly
			return binder.bind_context.CreateColumnReference(colref.column_names[0], colref.column_names[1]);
		} else {
			// otherwise check if we can turn this into a struct extract
			auto new_colref = make_uniq<ColumnRefExpression>(colref.column_names[0]);
			string other_error;
			auto qualified_colref = QualifyColumnName(colref.column_names[0], other_error);
			if (qualified_colref) {
				// we could: create a struct extract
				return CreateStructExtract(std::move(qualified_colref), colref.column_names[1]);
			}
			// we could not! Try creating an implicit struct_pack
			return CreateStructPack(colref);
		}
	} else {
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

		unique_ptr<ParsedExpression> result_expr;
		idx_t struct_extract_start;
		// first check if part1 is a catalog
		if (colref.column_names.size() > 3 &&
		    binder.HasMatchingBinding(colref.column_names[0], colref.column_names[1], colref.column_names[2],
		                              colref.column_names[3], error_message)) {
			// part1 is a catalog - the column reference is "catalog.schema.table.column"
			result_expr = binder.bind_context.CreateColumnReference(colref.column_names[0], colref.column_names[1],
			                                                        colref.column_names[2], colref.column_names[3]);
			struct_extract_start = 4;
		} else if (binder.HasMatchingBinding(colref.column_names[0], INVALID_SCHEMA, colref.column_names[1],
		                                     colref.column_names[2], error_message)) {
			// part1 is a catalog - the column reference is "catalog.table.column"
			result_expr = binder.bind_context.CreateColumnReference(colref.column_names[0], INVALID_SCHEMA,
			                                                        colref.column_names[1], colref.column_names[2]);
			struct_extract_start = 3;
		} else if (binder.HasMatchingBinding(colref.column_names[0], colref.column_names[1], colref.column_names[2],
		                                     error_message)) {
			// part1 is a schema - the column reference is "schema.table.column"
			// any additional fields are turned into struct_extract calls
			result_expr = binder.bind_context.CreateColumnReference(colref.column_names[0], colref.column_names[1],
			                                                        colref.column_names[2]);
			struct_extract_start = 3;
		} else if (binder.HasMatchingBinding(colref.column_names[0], colref.column_names[1], error_message)) {
			// part1 is a table
			// the column reference is "table.column"
			// any additional fields are turned into struct_extract calls
			result_expr = binder.bind_context.CreateColumnReference(colref.column_names[0], colref.column_names[1]);
			struct_extract_start = 2;
		} else {
			// part1 could be a column
			string col_error;
			result_expr = QualifyColumnName(colref.column_names[0], col_error);
			if (!result_expr) {
				// it is not! Try creating an implicit struct_pack
				return CreateStructPack(colref);
			}
			// it is! add the struct extract calls
			struct_extract_start = 1;
		}
		for (idx_t i = struct_extract_start; i < colref.column_names.size(); i++) {
			result_expr = CreateStructExtract(std::move(result_expr), colref.column_names[i]);
		}
		return result_expr;
	}
}

BindResult ExpressionBinder::BindExpression(ColumnRefExpression &colref_p, idx_t depth) {
	if (binder.GetBindingMode() == BindingMode::EXTRACT_NAMES) {
		return BindResult(make_uniq<BoundConstantExpression>(Value(LogicalType::SQLNULL)));
	}
	string error_message;
	auto expr = QualifyColumnName(colref_p, error_message);
	if (!expr) {
		return BindResult(binder.FormatError(colref_p, error_message));
	}
	expr->query_location = colref_p.query_location;

	// a generated column returns a generated expression, a struct on a column returns a struct extract
	if (expr->type != ExpressionType::COLUMN_REF) {
		auto alias = expr->alias;
		auto result = BindExpression(expr, depth);
		if (result.expression) {
			result.expression->alias = std::move(alias);
		}
		return result;
	}

	auto &colref = expr->Cast<ColumnRefExpression>();
	D_ASSERT(colref.IsQualified());
	auto &table_name = colref.GetTableName();

	// individual column reference
	// resolve to either a base table or a subquery expression
	// if it was a macro parameter, let macro_binding bind it to the argument
	// if it was a lambda parameter, let lambda_bindings bind it to the argument

	BindResult result;

	auto found_lambda_binding = false;
	if (lambda_bindings) {
		for (idx_t i = 0; i < lambda_bindings->size(); i++) {
			if (table_name == (*lambda_bindings)[i].alias) {
				result = (*lambda_bindings)[i].Bind(colref, i, depth);
				found_lambda_binding = true;
				break;
			}
		}
	}

	if (!found_lambda_binding) {
		if (binder.macro_binding && table_name == binder.macro_binding->alias) {
			result = binder.macro_binding->Bind(colref, depth);
		} else {
			result = binder.bind_context.BindColumn(colref, depth);
		}
	}

	if (!result.HasError()) {
		BoundColumnReferenceInfo ref;
		ref.name = colref.column_names.back();
		ref.query_location = colref.query_location;
		bound_columns.push_back(std::move(ref));
	} else {
		result.error = binder.FormatError(colref_p, result.error);
	}
	return result;
}

bool ExpressionBinder::QualifyColumnAlias(const ColumnRefExpression &colref) {
	// Only BaseSelectBinder will have a valid col alias map,
	// otherwise just return false
	return false;
}

} // namespace duckdb
