#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/planner/tableref/bound_table_function.hpp"
#include "duckdb/execution/expression_executor.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundTableRef> Binder::Bind(TableFunctionRef &ref) {
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
	auto result = make_unique<BoundTableFunction>(function, bind_index);
	// evalate the input parameters to the function
	for (auto &child : fexpr->children) {
		ConstantBinder binder(*this, context, "TABLE FUNCTION parameter");
		auto expr = binder.Bind(child);
		auto constant = ExpressionExecutor::EvaluateScalar(*expr);
		result->parameters.push_back(constant);
	}
	// perform the binding
	result->bind_data = function->function.bind(context, result->parameters, result->return_types, result->names);
	assert(result->return_types.size() == result->names.size());
	assert(result->return_types.size() > 0);
	// now add the table function to the bind context so its columns can be bound
	bind_context.AddGenericBinding(bind_index, ref.alias.empty() ? fexpr->function_name : ref.alias, result->names,
	                               result->return_types);
	return move(result);
}
