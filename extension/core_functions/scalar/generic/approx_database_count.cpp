#include "core_functions/scalar/generic_functions.hpp"

#include "duckdb/main/client_context.hpp"

namespace duckdb {

struct ApproxDatabaseCountBindData : public FunctionData {
	explicit ApproxDatabaseCountBindData(Value value_p) : value(std::move(value_p)) {
	}

	Value value;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<ApproxDatabaseCountBindData>(value);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<ApproxDatabaseCountBindData>();
		return Value::NotDistinctFrom(value, other.value);
	}
};

static void ApproxDatabaseCountFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<ApproxDatabaseCountBindData>();
	result.Reference(info.value);
}

unique_ptr<FunctionData> ApproxDatabaseCountBind(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
	auto &db_manager = DatabaseManager::Get(context);
	auto value = Value::UBIGINT(db_manager.ApproxDatabaseCount());
	return make_uniq<ApproxDatabaseCountBindData>(value);
}

ScalarFunction ApproxDatabaseCountFun::GetFunction() {
	auto fun = ScalarFunction({}, LogicalType::UBIGINT, ApproxDatabaseCountFunction, ApproxDatabaseCountBind);
	return fun;
}

} // namespace duckdb
