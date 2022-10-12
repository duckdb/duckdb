#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"

namespace duckdb {

struct UnionValueBindData : public FunctionData {
	UnionValueBindData() {
	}

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_unique<UnionValueBindData>();
	}
	bool Equals(const FunctionData &other_p) const override {
		return true;
	}
};

static void UnionValueFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	// Assign the new entries to the result vector
	UnionVector::GetMember(result, 0).Reference(args.data[0]);

	// Reset tags
	UnionVector::SetTags(result, 0, args.size());

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static unique_ptr<FunctionData> UnionValueBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {

	if (arguments.size() != 1) {
		throw Exception("union_value takes exactly one argument");
	}
	auto &child = arguments[0];

	if (child->alias.empty()) {
		throw BinderException("Need named argument for union tag, e.g. UNION_VALUE(a := b)");
	}

	child_list_t<LogicalType> union_members;

	union_members.push_back(make_pair(child->alias, child->return_type));

	bound_function.return_type = LogicalType::UNION(move(union_members));
	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

void UnionValueFun::RegisterFunction(BuiltinFunctions &set) {

	auto fun =
	    ScalarFunction("union_value", {}, LogicalTypeId::UNION, UnionValueFunction, UnionValueBind, nullptr, nullptr);
	fun.varargs = LogicalType::ANY;
	fun.serialize = VariableReturnBindData::Serialize;
	fun.deserialize = VariableReturnBindData::Deserialize;

	ScalarFunctionSet union_value("union_value");
	union_value.AddFunction(fun);
	set.AddFunction(union_value);
}

} // namespace duckdb
