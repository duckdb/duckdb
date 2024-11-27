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

enum class TableFunctionBindType { STANDARD_TABLE_FUNCTION, TABLE_IN_OUT_FUNCTION, TABLE_PARAMETER_FUNCTION };

static TableFunctionBindType GetTableFunctionBindType(TableFunctionCatalogEntry &table_function,
                                                      vector<unique_ptr<ParsedExpression>> &expressions) {
	// first check if all expressions are scalar
	// if they are we always bind as a standard table function
	bool all_scalar = true;
	for (auto &expr : expressions) {
		if (!expr->IsScalar()) {
			all_scalar = false;
			break;
		}
	}
	if (all_scalar) {
		return TableFunctionBindType::STANDARD_TABLE_FUNCTION;
	}
	// if we have non-scalar parameters - we need to look at the function definition to decide how to bind
	// if a function does not have an in_out_function defined, we need to bind as a standard table function regardless
	bool has_in_out_function = false;
	bool has_standard_table_function = false;
	bool has_table_parameter = false;
	for (idx_t function_idx = 0; function_idx < table_function.functions.Size(); function_idx++) {
		const auto &function = table_function.functions.GetFunctionReferenceByOffset(function_idx);
		for (auto &arg : function.arguments) {
			if (arg.id() == LogicalTypeId::TABLE) {
				has_table_parameter = true;
			}
		}
		if (function.in_out_function) {
			has_in_out_function = true;
		} else if (function.function || function.bind_replace) {
			has_standard_table_function = true;
		} else {
			throw InternalException("Function \"%s\" has neither in_out_function nor function defined",
			                        table_function.name);
		}
	}
	if (has_table_parameter) {
		if (table_function.functions.Size() != 1) {
			throw InternalException(
			    "Function \"%s\" has a TABLE parameter, and multiple function overloads - this is not supported",
			    table_function.name);
		}
		return TableFunctionBindType::TABLE_PARAMETER_FUNCTION;
	}
	if (has_in_out_function && has_standard_table_function) {
		throw InternalException("Function \"%s\" is both an in_out_function and a table function", table_function.name);
	}
	return has_in_out_function ? TableFunctionBindType::TABLE_IN_OUT_FUNCTION
	                           : TableFunctionBindType::STANDARD_TABLE_FUNCTION;
}

void Binder::BindTableInTableOutFunction(vector<unique_ptr<ParsedExpression>> &expressions,
                                         unique_ptr<BoundSubqueryRef> &subquery) {
	auto binder = Binder::CreateBinder(this->context, this);
	unique_ptr<QueryNode> subquery_node;
	// generate a subquery and bind that (i.e. UNNEST([1,2,3]) becomes UNNEST((SELECT [1,2,3]))
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list = std::move(expressions);
	select_node->from_table = make_uniq<EmptyTableRef>();
	subquery_node = std::move(select_node);
	binder->can_contain_nulls = true;
	auto node = binder->BindNode(*subquery_node);
	subquery = make_uniq<BoundSubqueryRef>(std::move(binder), std::move(node));
	MoveCorrelatedExpressions(*subquery->binder);
}

bool Binder::BindTableFunctionParameters(TableFunctionCatalogEntry &table_function,
                                         vector<unique_ptr<ParsedExpression>> &expressions,
                                         vector<LogicalType> &arguments, vector<Value> &parameters,
                                         named_parameter_map_t &named_parameters,
                                         unique_ptr<BoundSubqueryRef> &subquery, ErrorData &error) {
	auto bind_type = GetTableFunctionBindType(table_function, expressions);
	if (bind_type == TableFunctionBindType::TABLE_IN_OUT_FUNCTION) {
		// bind table in-out function
		BindTableInTableOutFunction(expressions, subquery);
		// fetch the arguments from the subquery
		arguments = subquery->subquery->types;
		return true;
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
		} else if (!child->alias.empty()) {
			// <name> => <expression> will set the alias of <expression> to <name>
			parameter_name = child->alias;
		}
		if (bind_type == TableFunctionBindType::TABLE_PARAMETER_FUNCTION && child->type == ExpressionType::SUBQUERY) {
			D_ASSERT(table_function.functions.Size() == 1);
			auto fun = table_function.functions.GetFunctionByOffset(0);
			if (table_function.functions.Size() != 1 || fun.arguments.empty() ||
			    fun.arguments[0].id() != LogicalTypeId::TABLE) {
				throw BinderException(
				    "Only table-in-out functions can have subquery parameters - %s only accepts constant parameters",
				    fun.name);
			}
			if (seen_subquery) {
				error = ErrorData("Table function can have at most one subquery parameter");
				return false;
			}
			auto binder = Binder::CreateBinder(this->context, this);
			binder->can_contain_nulls = true;
			auto &se = child->Cast<SubqueryExpression>();
			auto node = binder->BindNode(*se.subquery->node);
			subquery = make_uniq<BoundSubqueryRef>(std::move(binder), std::move(node));
			MoveCorrelatedExpressions(*subquery->binder);
			seen_subquery = true;
			arguments.emplace_back(LogicalTypeId::TABLE);
			parameters.emplace_back(Value());
			continue;
		}

		TableFunctionBinder binder(*this, context, table_function.name);
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
				error = ErrorData("Unnamed parameters cannot come after named parameters");
				return false;
			}
			arguments.emplace_back(constant.IsNull() ? LogicalType::SQLNULL : sql_type);
			parameters.emplace_back(std::move(constant));
		} else {
			named_parameters[parameter_name] = std::move(constant);
		}
	}
	return true;
}

static string GetAlias(const TableFunctionRef &ref) {
	if (!ref.alias.empty()) {
		return ref.alias;
	}
	if (ref.function && ref.function->type == ExpressionType::FUNCTION) {
		auto &function_expr = ref.function->Cast<FunctionExpression>();
		return function_expr.function_name;
	}
	return string();
}

unique_ptr<LogicalOperator> Binder::BindTableFunctionInternal(TableFunction &table_function,
                                                              const TableFunctionRef &ref, vector<Value> parameters,
                                                              named_parameter_map_t named_parameters,
                                                              vector<LogicalType> input_table_types,
                                                              vector<string> input_table_names) {
	auto function_name = GetAlias(ref);
	auto &column_name_alias = ref.column_name_alias;

	auto bind_index = GenerateTableIndex();
	// perform the binding
	unique_ptr<FunctionData> bind_data;
	vector<LogicalType> return_types;
	vector<string> return_names;
	if (table_function.bind || table_function.bind_replace) {
		TableFunctionBindInput bind_input(parameters, named_parameters, input_table_types, input_table_names,
		                                  table_function.function_info.get(), this, table_function, ref);
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
		for (idx_t i = 0; i < return_types.size(); i++) {
			get->AddColumnId(i);
		}
	}
	// now add the table function to the bind context so its columns can be bound
	bind_context.AddTableFunction(bind_index, function_name, return_names, return_types, get->GetMutableColumnIds(),
	                              get->GetTable().get());
	return std::move(get);
}

unique_ptr<LogicalOperator> Binder::BindTableFunction(TableFunction &function, vector<Value> parameters) {
	named_parameter_map_t named_parameters;
	vector<LogicalType> input_table_types;
	vector<string> input_table_names;

	TableFunctionRef ref;
	ref.alias = function.name;
	D_ASSERT(!ref.alias.empty());
	return BindTableFunctionInternal(function, ref, std::move(parameters), std::move(named_parameters),
	                                 std::move(input_table_types), std::move(input_table_names));
}

unique_ptr<BoundTableRef> Binder::Bind(TableFunctionRef &ref) {
	QueryErrorContext error_context(ref.query_location);

	D_ASSERT(ref.function->type == ExpressionType::FUNCTION);
	auto &fexpr = ref.function->Cast<FunctionExpression>();

	string catalog = fexpr.catalog;
	string schema = fexpr.schema;
	Binder::BindSchemaOrCatalog(context, catalog, schema);

	// fetch the function from the catalog
	auto &func_catalog = *GetCatalogEntry(CatalogType::TABLE_FUNCTION_ENTRY, catalog, schema, fexpr.function_name,
	                                      OnEntryNotFound::THROW_EXCEPTION, error_context);

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
	ErrorData error;
	if (!BindTableFunctionParameters(function, fexpr.children, arguments, parameters, named_parameters, subquery,
	                                 error)) {
		error.AddQueryLocation(ref);
		error.Throw();
	}

	// select the function based on the input parameters
	FunctionBinder function_binder(*this);
	auto best_function_idx = function_binder.BindFunction(function.name, function.functions, arguments, error);
	if (!best_function_idx.IsValid()) {
		error.AddQueryLocation(ref);
		error.Throw();
	}
	auto table_function = function.functions.GetFunctionByOffset(best_function_idx.GetIndex());

	// now check the named parameters
	BindNamedParameters(table_function.named_parameters, named_parameters, error_context, table_function.name);

	vector<LogicalType> input_table_types;
	vector<string> input_table_names;

	if (subquery) {
		input_table_types = subquery->subquery->types;
		input_table_names = subquery->subquery->names;
	} else if (table_function.in_out_function) {
		for (auto &param : parameters) {
			input_table_types.push_back(param.type());
			input_table_names.push_back(string());
		}
	}
	if (!parameters.empty()) {
		// cast the parameters to the type of the function
		for (idx_t i = 0; i < arguments.size(); i++) {
			auto target_type =
			    i < table_function.arguments.size() ? table_function.arguments[i] : table_function.varargs;

			if (target_type != LogicalType::ANY && target_type != LogicalType::POINTER &&
			    target_type.id() != LogicalTypeId::LIST && target_type != LogicalType::TABLE) {
				parameters[i] = parameters[i].CastAs(context, target_type);
			}
		}
	} else if (subquery) {
		for (idx_t i = 0; i < arguments.size(); i++) {
			auto target_type =
			    i < table_function.arguments.size() ? table_function.arguments[i] : table_function.varargs;

			if (target_type != LogicalType::ANY && target_type != LogicalType::POINTER &&
			    target_type.id() != LogicalTypeId::LIST) {
				input_table_types[i] = target_type;
			}
		}
	}

	auto get = BindTableFunctionInternal(table_function, ref, std::move(parameters), std::move(named_parameters),
	                                     std::move(input_table_types), std::move(input_table_names));
	auto table_function_ref = make_uniq<BoundTableFunction>(std::move(get));
	table_function_ref->subquery = std::move(subquery);
	return std::move(table_function_ref);
}

} // namespace duckdb
