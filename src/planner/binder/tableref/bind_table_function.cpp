#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/planner/expression_binder/select_binder.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/tableref/bound_table_function.hpp"
#include "duckdb/planner/tableref/bound_subqueryref.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"

namespace duckdb {

bool Binder::BindFunctionParameters(vector<unique_ptr<ParsedExpression>> &expressions, vector<LogicalType> &arguments,
                                    vector<Value> &parameters, named_parameter_map_t &named_parameters,
                                    unique_ptr<BoundSubqueryRef> &subquery, string &error) {
	bool seen_subquery = false;
	for (auto &child : expressions) {
		string parameter_name;

		// hack to make named parameters work
		if (child->type == ExpressionType::COMPARE_EQUAL) {
			// comparison, check if the LHS is a columnref
			auto &comp = (ComparisonExpression &)*child;
			if (comp.left->type == ExpressionType::COLUMN_REF) {
				auto &colref = (ColumnRefExpression &)*comp.left;
				if (!colref.IsQualified()) {
					parameter_name = colref.GetColumnName();
					child = move(comp.right);
				}
			}
		}
		if (child->type == ExpressionType::SUBQUERY) {
			if (seen_subquery) {
				error = "Table function can have at most one subquery parameter ";
				return false;
			}
			auto binder = Binder::CreateBinder(this->context, this, true);
			auto &se = (SubqueryExpression &)*child;
			auto node = binder->BindNode(*se.subquery->node);
			subquery = make_unique<BoundSubqueryRef>(move(binder), move(node));
			seen_subquery = true;
			arguments.emplace_back(LogicalTypeId::TABLE);
			continue;
		}
		ConstantBinder binder(*this, context, "TABLE FUNCTION parameter");
		LogicalType sql_type;
		auto expr = binder.Bind(child, &sql_type);
		if (!expr->IsFoldable()) {
			error = "Table function requires a constant parameter";
			return false;
		}
		auto constant = ExpressionExecutor::EvaluateScalar(*expr);
		if (parameter_name.empty()) {
			// unnamed parameter
			if (!named_parameters.empty()) {
				error = "Unnamed parameters cannot come after named parameters";
				return false;
			}
			arguments.emplace_back(sql_type);
			parameters.emplace_back(move(constant));
		} else {
			named_parameters[parameter_name] = move(constant);
		}
	}
	return true;
}

unique_ptr<BoundTableRef> Binder::Bind(TableFunctionRef &ref) {
	QueryErrorContext error_context(root_statement, ref.query_location);
	auto bind_index = GenerateTableIndex();

	D_ASSERT(ref.function->type == ExpressionType::FUNCTION);
	auto fexpr = (FunctionExpression *)ref.function.get();

	// evaluate the input parameters to the function
	vector<LogicalType> arguments;
	vector<Value> parameters;
	named_parameter_map_t named_parameters;
	unique_ptr<BoundSubqueryRef> subquery;
	string error;
	if (!BindFunctionParameters(fexpr->children, arguments, parameters, named_parameters, subquery, error)) {
		throw BinderException(FormatError(ref, error));
	}

	// fetch the function from the catalog
	auto &catalog = Catalog::GetCatalog(context);
	auto function =
	    catalog.GetEntry<TableFunctionCatalogEntry>(context, fexpr->schema, fexpr->function_name, false, error_context);

	// select the function based on the input parameters
	idx_t best_function_idx = Function::BindFunction(function->name, function->functions, arguments, error);
	if (best_function_idx == DConstants::INVALID_INDEX) {
		throw BinderException(FormatError(ref, error));
	}
	auto &table_function = function->functions[best_function_idx];

	// now check the named parameters
	BindNamedParameters(table_function.named_parameters, named_parameters, error_context, table_function.name);

	// cast the parameters to the type of the function
	for (idx_t i = 0; i < arguments.size(); i++) {
		if (table_function.arguments[i] != LogicalType::ANY && table_function.arguments[i] != LogicalType::TABLE &&
		    table_function.arguments[i] != LogicalType::POINTER &&
		    table_function.arguments[i].id() != LogicalTypeId::LIST) {
			parameters[i] = parameters[i].CastAs(table_function.arguments[i]);
		}
	}

	vector<LogicalType> input_table_types;
	vector<string> input_table_names;

	if (subquery) {
		input_table_types = subquery->subquery->types;
		input_table_names = subquery->subquery->names;
	}

	// perform the binding
	unique_ptr<FunctionData> bind_data;
	vector<LogicalType> return_types;
	vector<string> return_names;
	if (table_function.bind) {
		bind_data = table_function.bind(context, parameters, named_parameters, input_table_types, input_table_names,
		                                return_types, return_names);
	}
	D_ASSERT(return_types.size() == return_names.size());
	D_ASSERT(return_types.size() > 0);
	// overwrite the names with any supplied aliases
	for (idx_t i = 0; i < ref.column_name_alias.size() && i < return_names.size(); i++) {
		return_names[i] = ref.column_name_alias[i];
	}
	for (idx_t i = 0; i < return_names.size(); i++) {
		if (return_names[i].empty()) {
			return_names[i] = "C" + to_string(i);
		}
	}
	auto get = make_unique<LogicalGet>(bind_index, table_function, move(bind_data), return_types, return_names);
	// now add the table function to the bind context so its columns can be bound
	bind_context.AddTableFunction(bind_index, ref.alias.empty() ? fexpr->function_name : ref.alias, return_names,
	                              return_types, *get);
	if (subquery) {
		get->children.push_back(Binder::CreatePlan(*subquery));
	}

	return make_unique_base<BoundTableRef, BoundTableFunction>(move(get));
}

} // namespace duckdb
