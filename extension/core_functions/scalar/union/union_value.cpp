#include <string>
#include <utility>

#include "duckdb/common/vector/union_vector.hpp"
#include "core_functions/scalar/union_functions.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/enums/vector_type.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {
class ClientContext;
struct ExpressionState;

namespace {

struct UnionValueBindData : public FunctionData {
	UnionValueBindData() {
	}

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<UnionValueBindData>();
	}
	bool Equals(const FunctionData &other_p) const override {
		return true;
	}
};

void UnionValueFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// Assign the new entries to the result vector
	UnionVector::GetMember(result, 0).Reference(args.data[0]);

	// Set the result tag vector to a constant value
	auto &tag_vector = UnionVector::GetTags(result);
	tag_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::GetData<union_tag_t>(tag_vector)[0] = 0;
}

unique_ptr<FunctionData> UnionValueBind(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	if (arguments.size() != 1) {
		throw BinderException("union_value takes exactly one argument");
	}
	auto &child = arguments[0];

	if (child->GetAlias().empty()) {
		throw BinderException("Need named argument for union tag, e.g. UNION_VALUE(a := b)");
	}

	child_list_t<LogicalType> union_members;

	union_members.push_back(make_pair(child->GetAlias(), child->GetReturnType()));

	bound_function.SetReturnType(LogicalType::UNION(std::move(union_members)));
	return make_uniq<VariableReturnBindData>(bound_function.GetReturnType());
}

} // namespace

ScalarFunction UnionValueFun::GetFunction() {
	ScalarFunction fun("union_value", {}, LogicalTypeId::UNION, UnionValueFunction, UnionValueBind, nullptr, nullptr);
	fun.SetVarArgs(LogicalType::ANY);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	fun.SetSerializeCallback(VariableReturnBindData::Serialize);
	fun.SetDeserializeCallback(VariableReturnBindData::Deserialize);
	return fun;
}

} // namespace duckdb
