#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/planner/operator/logical_table_function.hpp"
#include "duckdb/execution/expression_executor.hpp"

using namespace std;

namespace duckdb {

unique_ptr<LogicalOperator> Binder::Bind(TableFunctionRef &ref) {
	auto bind_index = GenerateTableIndex();

	assert(ref.function->type == ExpressionType::FUNCTION);
	auto fexpr = (FunctionExpression *)ref.function.get();
	// parse the parameters of the function
	auto function =
	    Catalog::GetCatalog(context).GetEntry<TableFunctionCatalogEntry>(context, fexpr->schema, fexpr->function_name);

	// check if the argument lengths match
	if (fexpr->children.size() != function->function.arguments.size()) {
		throw CatalogException("Function with name %s exists, but argument length does not match! "
		                       "Expected %d arguments but got %d.",
		                       fexpr->function_name.c_str(), (int)function->function.arguments.size(),
		                       (int)fexpr->children.size());
	}
	vector<Value> parameters;
	vector<SQLType> return_types;
	vector<string> names;
	// evalate the input parameters to the function
	for (auto &child : fexpr->children) {
		ConstantBinder binder(*this, context, "TABLE FUNCTION parameter");
		auto expr = binder.Bind(child);
		auto constant = ExpressionExecutor::EvaluateScalar(*expr);
		parameters.push_back(constant);
	}
	// perform the binding
	auto bind_data = function->function.bind(context, parameters, return_types, names);
	auto bind_name = ref.alias.empty() ? fexpr->function_name : ref.alias;
	assert(return_types.size() == names.size());
	assert(return_types.size() > 0);
	// now add the table function to the bind context so its columns can be bound
	bind_context.AddGenericBinding(bind_index, bind_name, names, return_types);

	return make_unique<LogicalTableFunction>(function, bind_index, move(bind_data), move(parameters),
	                                         move(return_types), move(names));
}

}
