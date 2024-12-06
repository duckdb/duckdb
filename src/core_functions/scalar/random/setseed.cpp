#include "duckdb/core_functions/scalar/random_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/random_engine.hpp"

namespace duckdb {

struct SetseedBindData : public FunctionData {
	//! The client context for the function call
	ClientContext &context;

	explicit SetseedBindData(ClientContext &context) : context(context) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<SetseedBindData>(context);
	}

	bool Equals(const FunctionData &other_p) const override {
		return true;
	}
};

static void SetSeedFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<SetseedBindData>();
	auto &input = args.data[0];
	input.Flatten(args.size());

	auto input_seeds = FlatVector::GetData<double>(input);
	uint32_t half_max = NumericLimits<uint32_t>::Maximum() / 2;

	auto &random_engine = RandomEngine::Get(info.context);
	for (idx_t i = 0; i < args.size(); i++) {
		if (input_seeds[i] < -1.0 || input_seeds[i] > 1.0 || Value::IsNan(input_seeds[i])) {
			throw InvalidInputException("SETSEED accepts seed values between -1.0 and 1.0, inclusive");
		}
		auto norm_seed = LossyNumericCast<uint32_t>((input_seeds[i] + 1.0) * half_max);
		random_engine.SetSeed(norm_seed);
	}

	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::SetNull(result, true);
}

unique_ptr<FunctionData> SetSeedBind(ClientContext &context, ScalarFunction &bound_function,
                                     vector<unique_ptr<Expression>> &arguments) {
	return make_uniq<SetseedBindData>(context);
}

ScalarFunction SetseedFun::GetFunction() {
	ScalarFunction setseed("setseed", {LogicalType::DOUBLE}, LogicalType::SQLNULL, SetSeedFunction, SetSeedBind);
	setseed.stability = FunctionStability::VOLATILE;
	return setseed;
}

} // namespace duckdb
