#include "duckdb/function/scalar/math_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/random_engine.hpp"

namespace duckdb {

struct RandomBindData : public FunctionData {
	ClientContext &context;

	RandomBindData(ClientContext &context) : context(context) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<RandomBindData>(context);
	}
};

static void RandomFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 0);
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (RandomBindData &)*func_expr.bind_info;

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<double>(result);
	auto &random_engine = RandomEngine::Get(info.context);
	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i] = random_engine.NextRandom(0, 1);
	}
}

unique_ptr<FunctionData> RandomBind(ClientContext &context, ScalarFunction &bound_function,
                                    vector<unique_ptr<Expression>> &arguments) {
	return make_unique<RandomBindData>(context);
}

void RandomFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("random", {}, LogicalType::DOUBLE, RandomFunction, true, RandomBind));
}

} // namespace duckdb
