#include "function/scalar/math_functions.hpp"
#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "main/client_context.hpp"
#include "planner/expression/bound_function_expression.hpp"

using namespace duckdb;
using namespace std;

struct SetseedBindData : public FunctionData {
	//! The client context for the function call
	ClientContext &context;

	SetseedBindData(ClientContext &context) : context(context) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<SetseedBindData>(context);
	}
};

static void setseed_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                     Vector &result) {
	auto &info = (SetseedBindData &) *expr.bind_info;
	assert(input_count == 1 && inputs[0].type == TypeId::INTEGER);
	result.Initialize(TypeId::INTEGER);
	result.count = 1;

	int32_t seed = ((int32_t *)inputs[0].data)[0];
	info.context.random_engine.seed(seed);

	VectorOperations::Set(result, Value(seed));
}

unique_ptr<FunctionData> setseed_bind(BoundFunctionExpression &expr, ClientContext &context) {
	return make_unique<SetseedBindData>(context);
}

void Setseed::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("setseed", { SQLType::INTEGER }, SQLType::INTEGER, setseed_function, true, setseed_bind));
}
