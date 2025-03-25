#include "core_functions/scalar/random_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

struct ExtractVersionUuidOperator {
	template <typename INPUT_TYPE, typename RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		char uuid[36]; // Intentionally no initialize.
		BaseUUID::ToString(input, uuid);
		// UUIDv4 and UUIDv7 stores version as the 15-th uint8_t.
		return uuid[14] - '0';
	}
};

struct ExtractTimestampUuidOperator {
	template <typename INPUT_TYPE, typename RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		// Validate whether the given UUID is v7.
		const uint8_t version = (static_cast<uint8_t>((input.upper) >> 8) & 0xf0) >> 4;
		if (version != 7) {
			throw InvalidInputException("Given UUID is with version %u, not version 7.", version);
		}

		// UUID v7 begins with a 48 bit big-endian Unix Epoch timestamp with millisecond granularity.
		const int64_t upper = input.upper;
		int64_t unix_ts_milli = upper;
		unix_ts_milli = unix_ts_milli >> 16;

		static constexpr int64_t kMilliToMicro = 1000;
		const int64_t unix_ts_ms = kMilliToMicro * unix_ts_milli;
		return timestamp_t {unix_ts_ms};
	}
};

template <typename INPUT, typename OP>
static void ExtractVersionFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	auto &input = args.data[0];
	idx_t count = args.size();
	UnaryExecutor::ExecuteString<INPUT, uint32_t, OP>(input, result, count);
}

template <typename INPUT, typename OP>
static void ExtractTimestampFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	auto &input = args.data[0];
	idx_t count = args.size();
	UnaryExecutor::ExecuteString<INPUT, timestamp_t, OP>(input, result, count);
}

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

static void GenerateUUIDv4Function(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 0);
	auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<RandomLocalState>();

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<hugeint_t>(result);

	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i] = UUIDv4::GenerateRandomUUID(lstate.random_engine);
	}
}

static void GenerateUUIDv7Function(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 0);
	auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<RandomLocalState>();

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<hugeint_t>(result);

	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i] = UUIDv7::GenerateRandomUUID(lstate.random_engine);
	}
}

ScalarFunction UUIDFun::GetFunction() {
	return UUIDv4Fun::GetFunction();
}

ScalarFunction UUIDv4Fun::GetFunction() {
	ScalarFunction uuid_v4_function({}, LogicalType::UUID, GenerateUUIDv4Function, nullptr, nullptr, nullptr,
	                                RandomInitLocalState);
	// generate a random uuid v4
	uuid_v4_function.stability = FunctionStability::VOLATILE;
	return uuid_v4_function;
}

ScalarFunction UUIDv7Fun::GetFunction() {
	ScalarFunction uuid_v7_function({}, LogicalType::UUID, GenerateUUIDv7Function, nullptr, nullptr, nullptr,
	                                RandomInitLocalState);
	// generate a random uuid v7
	uuid_v7_function.stability = FunctionStability::VOLATILE;
	return uuid_v7_function;
}

ScalarFunction UUIDExtractVersionFun::GetFunction() {
	return ScalarFunction({LogicalType::UUID}, LogicalType::UINTEGER,
	                      ExtractVersionFunction<hugeint_t, ExtractVersionUuidOperator>);
}

ScalarFunction UUIDExtractTimestampFun::GetFunction() {
	return ScalarFunction({LogicalType::UUID}, LogicalType::TIMESTAMP_TZ,
	                      ExtractTimestampFunction<hugeint_t, ExtractTimestampUuidOperator>);
}

} // namespace duckdb
