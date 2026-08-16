#include "duckdb/common/vector/union_vector.hpp"
#include "core_functions/scalar/union_functions.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_argument_pack.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"

namespace duckdb {

namespace {

void UnionValueFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto &kwargs = ArgumentPack::GetInput(args.data[0]);

	// Assign the new entries to the result vector
	UnionVector::GetMember(result, 0).Reference(kwargs[0]);

	// Set the result tag vector to a constant value
	auto &tag_vector = UnionVector::GetTags(result);
	tag_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::GetData<union_tag_t>(tag_vector)[0] = 0;
}

unique_ptr<FunctionData> UnionValueBind(BindScalarFunctionInput &input) {
	const auto &args = input.GetArguments();

	const auto &kwargs = ArgumentPack::GetTypes(args[0]->GetReturnType());
	if (kwargs.size() != 1) {
		throw BinderException("union_value takes exactly one named argument");
	}

	auto &fun = input.GetBoundFunction();
	fun.SetReturnType(LogicalType::UNION(kwargs));

	return make_uniq<VariableReturnBindData>(fun.GetReturnType());
}

} // namespace

ScalarFunction UnionValueFun::GetFunction() {
	auto sig = FunctionSignature()
		.AddVarKeywordParameter("kwargs", LogicalType::ANY)
		.SetReturnType(LogicalTypeId::UNION);

	auto fun = ScalarFunction("union_value", std::move(sig))
		.SetFunctionCallback(UnionValueFunction)
		.SetBindCallback(UnionValueBind)
		.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING)
		.SetSerializeCallback(VariableReturnBindData::Serialize)
		.SetDeserializeCallback(VariableReturnBindData::Deserialize);

	return fun;
}

} // namespace duckdb
