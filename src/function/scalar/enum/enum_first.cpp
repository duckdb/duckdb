#include "duckdb/function/scalar/enum_functions.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

struct CurrentBindData : public FunctionData {
	ClientContext &context;

	explicit CurrentBindData(ClientContext &context) : context(context) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<CurrentBindData>(context);
	}
};

static void EnumFirstFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.GetTypes().size() == 1);
	auto &enum_vector = EnumType::GetValuesInsertOrder(input.GetTypes()[0]);
	auto val = Value(enum_vector.GetValue(0));
	result.Reference(val);
}

unique_ptr<FunctionData> BindEnumFunction(ClientContext &context, ScalarFunction &bound_function,
                                          vector<unique_ptr<Expression>> &arguments) {
	if (arguments[0]->return_type.id() != LogicalTypeId::ENUM) {
		throw std::runtime_error("This function needs an ENUM as an argument");
	}
	return make_unique<CurrentBindData>(context);
}

void EnumFirst::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("enum_first", {LogicalType::ANY}, LogicalType::VARCHAR, EnumFirstFunction, false,
	                               BindEnumFunction));
}

} // namespace duckdb
