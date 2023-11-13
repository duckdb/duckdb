#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/table_function_binder.hpp"
#include "duckdb/planner/expression_binder/select_binder.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/planner/tableref/bound_subqueryref.hpp"
#include "duckdb/planner/tableref/bound_table_function.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/function/table/read_csv.hpp"

namespace duckdb {

static bool IsTableInTableOutFunction(TableFunctionCatalogEntry &table_function) {
	auto fun = table_function.functions.GetFunctionByOffset(0);
	return table_function.functions.Size() == 1 && fun.arguments.size() == 1 &&
	       fun.arguments[0].id() == LogicalTypeId::TABLE;
}

bool Binder::BindTableInTableOutFunction(vector<unique_ptr<ParsedExpression>> &expressions,
                                         unique_ptr<BoundSubqueryRef> &subquery, string &error) {
	auto binder = Binder::CreateBinder(this->context, this, true);
	unique_ptr<QueryNode> subquery_node;
	if (expressions.size() == 1 && expressions[0]->type == ExpressionType::SUBQUERY) {
		// general case: argument is a subquery, bind it as part of the node
		auto &se = expressions[0]->Cast<SubqueryExpression>();
		subquery_node = std::move(se.subquery->node);
	} else {
		// special case: non-subquery parameter to table-in table-out function
		// generate a subquery and bind that (i.e. UNNEST([1,2,3]) becomes UNNEST((SELECT [1,2,3]))
		auto select_node = make_uniq<SelectNode>();
		select_node->select_list = std::move(expressions);
		select_node->from_table = make_uniq<EmptyTableRef>();
		subquery_node = std::move(select_node);
	}
	auto node = binder->BindNode(*subquery_node);
	subquery = make_uniq<BoundSubqueryRef>(std::move(binder), std::move(node));
	MoveCorrelatedExpressions(*subquery->binder);
	return true;
}

bool Binder::BindTableFunctionParameters(TableFunctionCatalogEntry &table_function,
                                         vector<unique_ptr<ParsedExpression>> &expressions,
                                         vector<LogicalType> &arguments, vector<Value> &parameters,
                                         named_parameter_map_t &named_parameters,
                                         unique_ptr<BoundSubqueryRef> &subquery, string &error) {
	if (IsTableInTableOutFunction(table_function)) {
		// special case binding for table-in table-out function
		arguments.emplace_back(LogicalTypeId::TABLE);
		return BindTableInTableOutFunction(expressions, subquery, error);
	}
	bool seen_subquery = false;
	for (auto &child : expressions) {
		string parameter_name;

		// hack to make named parameters work
		if (child->type == ExpressionType::COMPARE_EQUAL) {
			// comparison, check if the LHS is a columnref
			auto &comp = child->Cast<ComparisonExpression>();
			if (comp.left->type == ExpressionType::COLUMN_REF) {
				auto &colref = comp.left->Cast<ColumnRefExpression>();
				if (!colref.IsQualified()) {
					parameter_name = colref.GetColumnName();
					child = std::move(comp.right);
				}
			}
		}
		if (child->type == ExpressionType::SUBQUERY) {
			auto fun = table_function.functions.GetFunctionByOffset(0);
			if (table_function.functions.Size() != 1 || fun.arguments.empty() ||
			    fun.arguments[0].id() != LogicalTypeId::TABLE) {
				throw BinderException(
				    "Only table-in-out functions can have subquery parameters - %s only accepts constant parameters",
				    fun.name);
			}
			// this separate subquery binding path is only used by python_map
			// FIXME: this should be unified with `BindTableInTableOutFunction` above
			if (seen_subquery) {
				error = "Table function can have at most one subquery parameter ";
				return false;
			}
			auto binder = Binder::CreateBinder(this->context, this, true);
			auto &se = child->Cast<SubqueryExpression>();
			auto node = binder->BindNode(*se.subquery->node);
			subquery = make_uniq<BoundSubqueryRef>(std::move(binder), std::move(node));
			seen_subquery = true;
			arguments.emplace_back(LogicalTypeId::TABLE);
			parameters.emplace_back(
			    Value(LogicalType::INVALID)); // this is a dummy value so the lengths of arguments and parameter match
			continue;
		}

		TableFunctionBinder binder(*this, context);
		LogicalType sql_type;
		auto expr = binder.Bind(child, &sql_type);
		if (expr->HasParameter()) {
			throw ParameterNotResolvedException();
		}
		if (!expr->IsScalar()) {
			// should have been eliminated before
			throw InternalException("Table function requires a constant parameter");
		}
		auto constant = ExpressionExecutor::EvaluateScalar(context, *expr, true);
		if (parameter_name.empty()) {
			// unnamed parameter
			if (!named_parameters.empty()) {
				error = "Unnamed parameters cannot come after named parameters";
				return false;
			}
			arguments.emplace_back(sql_type);
			parameters.emplace_back(std::move(constant));
		} else {
			named_parameters[parameter_name] = std::move(constant);
		}
	}
	return true;
}

unique_ptr<LogicalOperator>
Binder::BindTableFunctionInternal(TableFunction &table_function, const string &function_name, vector<Value> parameters,
                                  named_parameter_map_t named_parameters, vector<LogicalType> input_table_types,
                                  vector<string> input_table_names, const vector<string> &column_name_alias,
                                  unique_ptr<ExternalDependency> external_dependency) {
	auto bind_index = GenerateTableIndex();
	// perform the binding
	unique_ptr<FunctionData> bind_data;
	vector<LogicalType> return_types;
	vector<string> return_names;
	if (table_function.bind || table_function.bind_replace) {
		TableFunctionBindInput bind_input(parameters, named_parameters, input_table_types, input_table_names,
		                                  table_function.function_info.get());
		if (table_function.bind_replace) {
			auto new_plan = table_function.bind_replace(context, bind_input);
			if (new_plan != nullptr) {
				return CreatePlan(*Bind(*new_plan));
			} else if (!table_function.bind) {
				throw BinderException("Failed to bind \"%s\": nullptr returned from bind_replace without bind function",
				                      table_function.name);
			}
		}
		bind_data = table_function.bind(context, bind_input, return_types, return_names);
		if (table_function.name == "pandas_scan" || table_function.name == "arrow_scan") {
			auto &arrow_bind = bind_data->Cast<PyTableFunctionData>();
			arrow_bind.external_dependency = std::move(external_dependency);
		}
		if (table_function.name == "read_csv" || table_function.name == "read_csv_auto") {
			auto &csv_bind = bind_data->Cast<ReadCSVData>();
			if (csv_bind.single_threaded) {
				table_function.extra_info = "(Single-Threaded)";
			} else {
				table_function.extra_info = "(Multi-Threaded)";
			}
		}
	} else {
		throw InvalidInputException("Cannot call function \"%s\" directly - it has no bind function",
		                            table_function.name);
	}
	if (return_types.size() != return_names.size()) {
		throw InternalException("Failed to bind \"%s\": return_types/names must have same size", table_function.name);
	}
	if (return_types.empty()) {
		throw InternalException("Failed to bind \"%s\": Table function must return at least one column",
		                        table_function.name);
	}
	// overwrite the names with any supplied aliases
	for (idx_t i = 0; i < column_name_alias.size() && i < return_names.size(); i++) {
		return_names[i] = column_name_alias[i];
	}
	for (idx_t i = 0; i < return_names.size(); i++) {
		if (return_names[i].empty()) {
			return_names[i] = "C" + to_string(i);
		}
	}

	auto get = make_uniq<LogicalGet>(bind_index, table_function, std::move(bind_data), return_types, return_names);
	get->parameters = parameters;
	get->named_parameters = named_parameters;
	get->input_table_types = input_table_types;
	get->input_table_names = input_table_names;
	if (table_function.in_out_function && !table_function.projection_pushdown) {
		get->column_ids.reserve(return_types.size());
		for (idx_t i = 0; i < return_types.size(); i++) {
			get->column_ids.push_back(i);
		}
	}
	// now add the table function to the bind context so its columns can be bound
	bind_context.AddTableFunction(bind_index, function_name, return_names, return_types, get->column_ids,
	                              get->GetTable().get());
	return std::move(get);
}

unique_ptr<LogicalOperator> Binder::BindTableFunction(TableFunction &function, vector<Value> parameters) {
	named_parameter_map_t named_parameters;
	vector<LogicalType> input_table_types;
	vector<string> input_table_names;
	vector<string> column_name_aliases;
	return BindTableFunctionInternal(function, function.name, std::move(parameters), std::move(named_parameters),
	                                 std::move(input_table_types), std::move(input_table_names), column_name_aliases,
	                                 nullptr);
}

unique_ptr<BoundTableRef> Binder::Bind(TableFunctionRef &ref) {
	QueryErrorContext error_context(root_statement, ref.query_location);

	D_ASSERT(ref.function->type == ExpressionType::FUNCTION);
	auto &fexpr = ref.function->Cast<FunctionExpression>();

	// fetch the function from the catalog
	auto &func_catalog = Catalog::GetEntry(context, CatalogType::TABLE_FUNCTION_ENTRY, fexpr.catalog, fexpr.schema,
	                                       fexpr.function_name, error_context);

	if (func_catalog.type == CatalogType::TABLE_MACRO_ENTRY) {
		auto &macro_func = func_catalog.Cast<TableMacroCatalogEntry>();
		auto query_node = BindTableMacro(fexpr, macro_func, 0);
		D_ASSERT(query_node);

		auto binder = Binder::CreateBinder(context, this);
		binder->can_contain_nulls = true;

		binder->alias = ref.alias.empty() ? "unnamed_query" : ref.alias;
		auto query = binder->BindNode(*query_node);

		idx_t bind_index = query->GetRootIndex();
		// string alias;
		string alias = (ref.alias.empty() ? "unnamed_query" + to_string(bind_index) : ref.alias);

		auto result = make_uniq<BoundSubqueryRef>(std::move(binder), std::move(query));
		// remember ref here is TableFunctionRef and NOT base class
		bind_context.AddSubquery(bind_index, alias, ref, *result->subquery);
		MoveCorrelatedExpressions(*result->binder);
		return std::move(result);
	}
	D_ASSERT(func_catalog.type == CatalogType::TABLE_FUNCTION_ENTRY);
	auto &function = func_catalog.Cast<TableFunctionCatalogEntry>();

	// evaluate the input parameters to the function
	vector<LogicalType> arguments;
	vector<Value> parameters;
	named_parameter_map_t named_parameters;
	unique_ptr<BoundSubqueryRef> subquery;
	string error;
	if (!BindTableFunctionParameters(function, fexpr.children, arguments, parameters, named_parameters, subquery,
	                                 error)) {
		throw BinderException(FormatError(ref, error));
	}

	// select the function based on the input parameters
	FunctionBinder function_binder(context);
	idx_t best_function_idx = function_binder.BindFunction(function.name, function.functions, arguments, error);
	if (best_function_idx == DConstants::INVALID_INDEX) {
		throw BinderException(FormatError(ref, error));
	}
	auto table_function = function.functions.GetFunctionByOffset(best_function_idx);

	// now check the named parameters
	BindNamedParameters(table_function.named_parameters, named_parameters, error_context, table_function.name);

	// cast the parameters to the type of the function
	for (idx_t i = 0; i < arguments.size(); i++) {
		auto target_type = i < table_function.arguments.size() ? table_function.arguments[i] : table_function.varargs;

		if (target_type != LogicalType::ANY && target_type != LogicalType::TABLE &&
		    target_type != LogicalType::POINTER && target_type.id() != LogicalTypeId::LIST) {
			parameters[i] = parameters[i].CastAs(context, target_type);
		}
	}

	vector<LogicalType> input_table_types;
	vector<string> input_table_names;

	if (subquery) {
		input_table_types = subquery->subquery->types;
		input_table_names = subquery->subquery->names;
	}
	auto get = BindTableFunctionInternal(table_function, ref.alias.empty() ? fexpr.function_name : ref.alias,
	                                     std::move(parameters), std::move(named_parameters),
	                                     std::move(input_table_types), std::move(input_table_names),
	                                     ref.column_name_alias, std::move(ref.external_dependency));
	if (subquery) {
		get->children.push_back(Binder::CreatePlan(*subquery));
	}

	return make_uniq_base<BoundTableRef, BoundTableFunction>(std::move(get));
}

} // namespace duckdb
