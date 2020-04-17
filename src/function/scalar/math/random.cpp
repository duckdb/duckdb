#include "duckdb/function/scalar/math_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include <random>

using namespace duckdb;
using namespace std;

struct RandomBindData : public FunctionData {
	ClientContext &context;
	uniform_real_distribution<double> dist;

	RandomBindData(ClientContext &context, uniform_real_distribution<double> dist) : context(context), dist(dist) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<RandomBindData>(context, dist);
	}
};

static void random_function(DataChunk &args, ExpressionState &state, Vector &result) {
	assert(args.column_count() == 0);
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (RandomBindData &)*func_expr.bind_info;

	result.vector_type = VectorType::FLAT_VECTOR;
	auto result_data = FlatVector::GetData<double>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i] = info.dist(info.context.random_engine);
	}
}

unique_ptr<FunctionData> random_bind(BoundFunctionExpression &expr, ClientContext &context) {
	uniform_real_distribution<double> dist(0, 1);
	return make_unique<RandomBindData>(context, move(dist));
}

void RandomFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("random", {}, SQLType::DOUBLE, random_function, true, random_bind));
}
