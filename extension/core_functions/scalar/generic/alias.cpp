#include "core_functions/scalar/generic_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

namespace {
struct AliasBindData : public FunctionData {
	explicit AliasBindData(string alias_p) : alias(std::move(alias_p)) {
	}

	string alias;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<AliasBindData>(alias);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<AliasBindData>();
		return alias == other.alias;
	}
};

unique_ptr<FunctionData> AliasBind(BindScalarFunctionInput &input) {
	return make_uniq<AliasBindData>(input.GetArguments()[0]->GetName());
}
} // namespace

static void AliasFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &bind_data = func_expr.bind_info->Cast<AliasBindData>();
	Value v(state.expr.GetAlias().empty() ? bind_data.alias : state.expr.GetAlias());
	result.Reference(v, count_t(args.size()));
}

ScalarFunction AliasFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::ANY}, LogicalType::VARCHAR, AliasFunction, AliasBind);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return fun;
}

} // namespace duckdb
