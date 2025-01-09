#include "core_functions/scalar/random_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

struct RandomLocalState : public FunctionLocalState {
	explicit RandomLocalState(uint64_t seed) : random_engine(0) {
		random_engine.SetSeed(seed);
	}

	RandomEngine random_engine;
};

static void RandomFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 0);
	auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<RandomLocalState>();

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
	return make_uniq<RandomLocalState>(random_engine.NextRandomInteger64());
}

ScalarFunction RandomFun::GetFunction() {
	ScalarFunction random("random", {}, LogicalType::DOUBLE, RandomFunction, nullptr, nullptr, nullptr,
	                      RandomInitLocalState);
	random.stability = FunctionStability::VOLATILE;
	return random;
}

static void GenerateUUIDFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 0);
	auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<RandomLocalState>();

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<hugeint_t>(result);

	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i] = UUID::GenerateRandomUUID(lstate.random_engine);
	}
}

ScalarFunction UUIDFun::GetFunction() {
	ScalarFunction uuid_function({}, LogicalType::UUID, GenerateUUIDFunction, nullptr, nullptr, nullptr,
	                             RandomInitLocalState);
	// generate a random uuid
	uuid_function.stability = FunctionStability::VOLATILE;
	return uuid_function;
}

} // namespace duckdb
