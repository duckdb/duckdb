#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/tableref/bound_table_function.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/algorithm.hpp"

namespace duckdb {
using namespace std;

unique_ptr<BoundTableRef> Binder::Bind(TableFunctionRef &ref) {
	auto bind_index = GenerateTableIndex();

	assert(ref.function->type == ExpressionType::FUNCTION);
	auto fexpr = (FunctionExpression *)ref.function.get();

	// evalate the input parameters to the function
	vector<LogicalType> arguments;
	vector<Value> parameters;
	unordered_map<string, Value> named_parameters;
	for (auto &child : fexpr->children) {
		string parameter_name;

		ConstantBinder binder(*this, context, "TABLE FUNCTION parameter");
		if (child->type == ExpressionType::COMPARE_EQUAL) {
			// comparison, check if the LHS is a columnref
			auto &comp = (ComparisonExpression &)*child;
			if (comp.left->type == ExpressionType::COLUMN_REF) {
				auto &colref = (ColumnRefExpression &)*comp.left;
				if (colref.table_name.empty()) {
					parameter_name = colref.column_name;
					child = move(comp.right);
				}
			}
		}
		LogicalType sql_type;
		auto expr = binder.Bind(child, &sql_type);
		if (!expr->IsFoldable()) {
			throw BinderException("Named parameter requires a constant parameter");
		}
		auto constant = ExpressionExecutor::EvaluateScalar(*expr);
		if (parameter_name.empty()) {
			// unnamed parameter
			if (named_parameters.size() > 0) {
				throw BinderException("Unnamed parameters cannot come after named parameters");
			}
			arguments.push_back(sql_type);
			parameters.push_back(move(constant));
		} else {
			named_parameters[parameter_name] = move(constant);
		}
	}
	// fetch the function from the catalog
	auto function =
	    Catalog::GetCatalog(context).GetEntry<TableFunctionCatalogEntry>(context, fexpr->schema, fexpr->function_name);

	// select the function based on the input parameters
	idx_t best_function_idx = Function::BindFunction(function->name, function->functions, arguments);
	auto &table_function = function->functions[best_function_idx];

	// now check the named parameters
	for (auto &kv : named_parameters) {
		auto entry = table_function.named_parameters.find(kv.first);
		if (entry == table_function.named_parameters.end()) {
			throw BinderException("Invalid named parameter \"%s\" for function %s", kv.first, table_function.name);
		}
		kv.second = kv.second.CastAs(entry->second);
	}

	// cast the parameters to the type of the function
	for (idx_t i = 0; i < arguments.size(); i++) {
		if (table_function.arguments[i] != LogicalType::ANY) {
			parameters[i] = parameters[i].CastAs(table_function.arguments[i]);
		}
	}

	// perform the binding
	unique_ptr<FunctionData> bind_data;
	vector<LogicalType> return_types;
	vector<string> return_names;
	if (table_function.bind) {
		bind_data = table_function.bind(context, parameters, named_parameters, return_types, return_names);
	}
	assert(return_types.size() == return_names.size());
	assert(return_types.size() > 0);
	// overwrite the names with any supplied aliases
	for (idx_t i = 0; i < ref.column_name_alias.size() && i < return_names.size(); i++) {
		return_names[i] = ref.column_name_alias[i];
	}
	auto get = make_unique<LogicalGet>(bind_index, table_function, move(bind_data), return_types, return_names);
	// now add the table function to the bind context so its columns can be bound
	bind_context.AddTableFunction(bind_index, ref.alias.empty() ? fexpr->function_name : ref.alias, return_names,
	                              return_types, *get);

	return make_unique_base<BoundTableRef, BoundTableFunction>(move(get));
}

} // namespace duckdb
