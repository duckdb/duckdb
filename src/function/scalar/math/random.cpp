#include "duckdb/function/scalar/math_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/function/scalar/uuid_functions.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

struct RandomLocalState : public FunctionLocalState {
	explicit RandomLocalState(uint32_t seed) : random_engine(seed) {
	}

	RandomEngine random_engine;
};

static void RandomFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 0);
	auto &lstate = (RandomLocalState &)*ExecuteFunctionState::GetFunctionState(state);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<double>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i] = lstate.random_engine.NextRandom();
	}
}

static unique_ptr<FunctionLocalState> RandomInitLocalState(ExpressionState &state, const BoundFunctionExpression &expr,
                                                           FunctionData *bind_data) {
	auto &random_engine = RandomEngine::Get(state.GetContext());
	lock_guard<mutex> guard(random_engine.lock);
	return make_unique<RandomLocalState>(random_engine.NextRandomInteger());
}

void RandomFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction random("random", {}, LogicalType::DOUBLE, RandomFunction, nullptr, nullptr, nullptr,
	                      RandomInitLocalState);
	random.side_effects = FunctionSideEffects::HAS_SIDE_EFFECTS;
	set.AddFunction(random);
}

static void GenerateUUIDFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 0);
	auto &lstate = (RandomLocalState &)*ExecuteFunctionState::GetFunctionState(state);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<hugeint_t>(result);

	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i] = UUID::GenerateRandomUUID(lstate.random_engine);
	}
}

void UUIDFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction uuid_function({}, LogicalType::UUID, GenerateUUIDFunction, nullptr, nullptr, nullptr,
	                             RandomInitLocalState);
	// generate a random uuid
	uuid_function.side_effects = FunctionSideEffects::HAS_SIDE_EFFECTS;
	set.AddFunction({"uuid", "gen_random_uuid"}, uuid_function);
}

} // namespace duckdb
