#include "duckdb/function/scalar/uuid_functions.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

struct UUIDRandomBindData : public FunctionData {
	ClientContext &context;
	std::uniform_int_distribution<uint32_t> dist;

	UUIDRandomBindData(ClientContext &context, std::uniform_int_distribution<uint32_t> dist)
	    : context(context), dist(dist) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_unique<UUIDRandomBindData>(context, dist);
	}
	bool Equals(const FunctionData &other_p) const override {
		return true;
	}
};

static unique_ptr<FunctionData> UUIDRandomBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	std::uniform_int_distribution<uint32_t> dist;
	return make_unique<UUIDRandomBindData>(context, dist);
}

static void GenerateUUIDFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 0);
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (UUIDRandomBindData &)*func_expr.bind_info;

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<hugeint_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		uint8_t bytes[16];
		for (int i = 0; i < 16; i += 4) {
			*reinterpret_cast<uint32_t *>(bytes + i) = info.dist(info.context.random_engine);
		}
		// variant must be 10xxxxxx
		bytes[8] &= 0xBF;
		bytes[8] |= 0x80;
		// version must be 0100xxxx
		bytes[6] &= 0x4F;
		bytes[6] |= 0x40;

		result_data[i].upper = 0;
		result_data[i].upper |= ((int64_t)bytes[0] << 56);
		result_data[i].upper |= ((int64_t)bytes[1] << 48);
		result_data[i].upper |= ((int64_t)bytes[3] << 40);
		result_data[i].upper |= ((int64_t)bytes[4] << 32);
		result_data[i].upper |= ((int64_t)bytes[5] << 24);
		result_data[i].upper |= ((int64_t)bytes[6] << 16);
		result_data[i].upper |= ((int64_t)bytes[7] << 8);
		result_data[i].upper |= bytes[8];
		result_data[i].lower = 0;
		result_data[i].lower |= ((uint64_t)bytes[8] << 56);
		result_data[i].lower |= ((uint64_t)bytes[9] << 48);
		result_data[i].lower |= ((uint64_t)bytes[10] << 40);
		result_data[i].lower |= ((uint64_t)bytes[11] << 32);
		result_data[i].lower |= ((uint64_t)bytes[12] << 24);
		result_data[i].lower |= ((uint64_t)bytes[13] << 16);
		result_data[i].lower |= ((uint64_t)bytes[14] << 8);
		result_data[i].lower |= bytes[15];
	}
}

void UUIDFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction uuid_function({}, LogicalType::UUID, GenerateUUIDFunction, false, true, UUIDRandomBind);
	// generate a random uuid
	set.AddFunction({"uuid", "gen_random_uuid"}, uuid_function);
}

} // namespace duckdb
