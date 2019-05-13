#include "main/client_context.hpp"
#include "main/database.hpp"
#include "parser/expression/function_expression.hpp"
#include "parser/tableref/table_function.hpp"
#include "planner/binder.hpp"
#include "planner/expression_binder/constant_binder.hpp"
#include "planner/tableref/bound_table_function.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundTableRef> Binder::Bind(TableFunction &ref) {
	auto bind_index = GenerateTableIndex();

	assert(ref.function->type == ExpressionType::FUNCTION);
	auto function_definition = (FunctionExpression *)ref.function.get();
	// parse the parameters of the function
	auto function = context.catalog.GetTableFunction(context.ActiveTransaction(), function_definition);
	auto result = make_unique<BoundTableFunction>(function, bind_index);
	for (auto &child : function_definition->children) {
		ConstantBinder binder(*this, context, "TABLE FUNCTION parameter");
		result->parameters.push_back(binder.Bind(child));
	}
	bind_context.AddTableFunction(bind_index, ref.alias.empty() ? function_definition->function_name : ref.alias,
	                              function);
	return move(result);
}
