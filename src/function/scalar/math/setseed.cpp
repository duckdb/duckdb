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

static void setseed_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count,
                             BoundFunctionExpression &expr, Vector &result) {
	auto &info = (SetseedBindData &)*expr.bind_info;
	assert(input_count == 1 && inputs[0].type == TypeId::DOUBLE);
	result.Initialize(TypeId::INTEGER);
	result.nullmask.set();
	result.sel_vector = inputs[0].sel_vector;
	result.count = inputs[0].count;

	auto input_seeds = ((double *)inputs[0].data);
	uint32_t half_max = numeric_limits<uint32_t>::max() / 2;
	VectorOperations::Exec(result, [&](index_t i, index_t k) {
		if (input_seeds[i] < -1.0 || input_seeds[i] > 1.0) {
			throw Exception("SETSEED accepts seed values between -1.0 and 1.0, inclusive");
		}
		uint32_t norm_seed = (input_seeds[i] + 1.0) * half_max;
		info.context.random_engine.seed(norm_seed);
	});
}

unique_ptr<FunctionData> setseed_bind(BoundFunctionExpression &expr, ClientContext &context) {
	return make_unique<SetseedBindData>(context);
}

void SetseedFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(
	    ScalarFunction("setseed", {SQLType::DOUBLE}, SQLType::SQLNULL, setseed_function, true, setseed_bind));
}
