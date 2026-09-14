#include "core_functions/scalar/generic_functions.hpp"
#include "duckdb/function/scalar/generic_common.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

namespace {
unique_ptr<FunctionData> AliasBind(BindScalarFunctionInput &input) {
	return make_uniq<AliasBindData>(input.GetArguments()[0]->GetName());
}

void AliasSerialize(Serializer &serializer, const optional_ptr<FunctionData> data, const BoundScalarFunction &) {
	serializer.WriteProperty(100, "alias", data->Cast<AliasBindData>().alias);
}

unique_ptr<FunctionData> AliasDeserialize(Deserializer &deserializer, BoundScalarFunction &) {
	return make_uniq<AliasBindData>(deserializer.ReadProperty<Identifier>(100, "alias"));
}
} // namespace

static void AliasFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &bind_data = func_expr.BindInfo()->Cast<AliasBindData>();
	Value v(state.expr.GetAlias().empty() ? bind_data.alias : state.expr.GetAlias());
	result.Reference(v, count_t(args.size()));
}

ScalarFunction AliasFun::GetFunction() {
	auto fun = ScalarFunction({}, LogicalType::VARCHAR, AliasFunction, AliasBind);
	fun.GetSignature().AddParameter("expr", LogicalType::ANY);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	fun.GetProperties().SetRequiresExpressionNames(true);
	fun.SetSerializeCallback(AliasSerialize);
	fun.SetDeserializeCallback(AliasDeserialize);
	return fun;
}

} // namespace duckdb
