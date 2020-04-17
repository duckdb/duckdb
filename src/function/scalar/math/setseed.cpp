#include "duckdb/function/scalar/math_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

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

static void setseed_function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (SetseedBindData &)*func_expr.bind_info;
	auto &input = args.data[0];
	input.Normalify(args.size());

	auto input_seeds = FlatVector::GetData<double>(input);
	uint32_t half_max = numeric_limits<uint32_t>::max() / 2;

	for (idx_t i = 0; i < args.size(); i++) {
		if (input_seeds[i] < -1.0 || input_seeds[i] > 1.0) {
			throw Exception("SETSEED accepts seed values between -1.0 and 1.0, inclusive");
		}
		uint32_t norm_seed = (input_seeds[i] + 1.0) * half_max;
		info.context.random_engine.seed(norm_seed);
	}

	result.vector_type = VectorType::CONSTANT_VECTOR;
	ConstantVector::SetNull(result, true);
}

unique_ptr<FunctionData> setseed_bind(BoundFunctionExpression &expr, ClientContext &context) {
	return make_unique<SetseedBindData>(context);
}

void SetseedFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(
	    ScalarFunction("setseed", {SQLType::DOUBLE}, SQLType::SQLNULL, setseed_function, true, setseed_bind));
}
