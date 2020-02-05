#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

using namespace duckdb;
using namespace std;


// FIXME this really needs to be somewhere else
struct StructPackBindData : public FunctionData {
	vector<string> names;

	StructPackBindData(vector<string> names) : names(names) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<StructPackBindData>(names);
	}
};

static void struct_pack_fun(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (StructPackBindData &)*func_expr.bind_info;

	assert(input.column_count == info.names.size());
	for (size_t i = 0; i < input.column_count; i++) {
		auto new_child = make_unique<Vector>();
		new_child->Reference(input.data[i]);
		result.children.push_back(pair<string, unique_ptr<Vector>>(info.names[i], move(new_child)));
	}
	result.count = input.data[0].count;
}


BindResult ExpressionBinder::BindExpression(FunctionExpression &function, index_t depth) {
	// TODO actually have struct_pack in catalog
	if (function.function_name == "struct_pack") {
		string error;
		vector<string> names;

		for (index_t i = 0; i < function.children.size(); i++) {
			// TODO alias cannot be empty and needs to be unique!
			// TODO verify this
			names.push_back(function.children[i]->alias);
			BindChild(function.children[i], depth, error);
		}
		if (!error.empty()) {
			return BindResult(error);
		}
		// all children bound successfully
		// extract the children and types
		vector<SQLType> arguments;
		vector<unique_ptr<Expression>> children;

		auto stype = SQLType::STRUCT;

		for (index_t i = 0; i < function.children.size(); i++) {
			auto &child = (BoundExpression &)*function.children[i];
			stype.child_type.push_back(pair<string, SQLType>(names[i], child.sql_type));

			arguments.push_back(child.sql_type);
			child.expr->alias = function.children[i]->alias;
			children.push_back(move(child.expr));
		}

		// TODO need to construct the sqltype and put into bind_data to verify vector types
		// TODO need the arg alias in here
		ScalarFunction pack_fun(arguments, stype, struct_pack_fun);
		auto result = make_unique<BoundFunctionExpression>(TypeId::STRUCT, pack_fun, false);
		result->children = move(children);
		result->bind_info = make_unique<StructPackBindData>(names);
		auto return_type = result->function.return_type;
		return BindResult(move(result), return_type);
	}

	// lookup the function in the catalog
	auto func = context.catalog.GetFunction(context.ActiveTransaction(), function.schema, function.function_name);
	if (func->type == CatalogType::SCALAR_FUNCTION) {
		// scalar function
		return BindFunction(function, (ScalarFunctionCatalogEntry *)func, depth);
	} else {
		// aggregate function
		return BindAggregate(function, (AggregateFunctionCatalogEntry *)func, depth);
	}
}


//static void struct_extract_fun(DataChunk &input, ExpressionState &state, Vector &result) {
//
//}


BindResult ExpressionBinder::BindFunction(FunctionExpression &function, ScalarFunctionCatalogEntry *func,
                                          index_t depth) {
	// bind the children of the function expression
	string error;
	for (index_t i = 0; i < function.children.size(); i++) {
		BindChild(function.children[i], depth, error);
	}
	if (!error.empty()) {
		return BindResult(error);
	}
	// all children bound successfully
	// extract the children and types
	vector<SQLType> arguments;
	vector<unique_ptr<Expression>> children;
	for (index_t i = 0; i < function.children.size(); i++) {
		auto &child = (BoundExpression &)*function.children[i];
		arguments.push_back(child.sql_type);
		children.push_back(move(child.expr));
	}

//
//	if (function.function_name == "struct_extract") { // FIXME not here!
//		vector<SQLType> arguments;
//		// first child needs to be anything, second child needs to be varchar
//		if (children.size() != 2) {
//			return BindResult("struct_extract needs two arguments, second one needs to be a string");
//		}
//		ScalarFunction extract_fun("struct_extract", arguments, arguments[0], struct_extract_fun);
//		auto result = make_unique<BoundFunctionExpression>(children[0]->return_type, extract_fun, false);
//		auto return_type = result->function.return_type;
//		return BindResult(move(result), return_type);
//	}

	auto result = ScalarFunction::BindScalarFunction(context, *func, arguments, move(children), function.is_operator);
	auto return_type = result->function.return_type;
	return BindResult(move(result), return_type);
}

BindResult ExpressionBinder::BindAggregate(FunctionExpression &expr, AggregateFunctionCatalogEntry *function,
                                           index_t depth) {
	return BindResult(UnsupportedAggregateMessage());
}

string ExpressionBinder::UnsupportedAggregateMessage() {
	return "Aggregate functions are not supported here";
}
