#include "planner/binder.hpp"
#include "parser/tableref/table_function.hpp"
#include "planner/tableref/bound_table_functionref.hpp"
#include "parser/expression/function_expression.hpp"
#include "main/client_context.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundTableRef> Binder::Bind(TableFunction &ref) {
	auto result = make_unique<BoundTableFunction>();
	result->bind_index = GenerateTableIndex();

	assert(ref.function->type == ExpressionType::FUNCTION);
	auto &function_definition = (FunctionExpression &)*ref.function;
	// parse the parameters of the function
	for(auto &child : function_definition->children) {
		LimitBinder binder(*this, context);
		result->parameters.push_back(binder.BindAndResolveType(*child));
	}
	result->function = context.db.catalog.GetTableFunction(context.ActiveTransaction(), function_definition);
	bind_context.AddTableFunction(result->table_index,
	                              ref.alias.empty() ? function_definition->function_name : ref.alias,
								  result->function);
	return move(result);
}
