#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"

namespace duckdb {

static unique_ptr<FunctionData> UnionTagBind(ClientContext &context, ScalarFunction &bound_function,
                                             vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 1);
	if (arguments[0]->return_type.id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}
	D_ASSERT(LogicalTypeId::UNION == arguments[0]->return_type.id());

	auto member_count = UnionType::GetMemberCount(arguments[0]->return_type);
	if (member_count == 0) {
		// this should never happen, empty unions are not allowed
		throw InternalException("Can't get tags from an empty union");
	}
	bound_function.arguments[0] = arguments[0]->return_type;

	auto varchar_vector = Vector(LogicalType::VARCHAR, member_count);
	for (idx_t i = 0; i < member_count; i++) {
		auto str = string_t(UnionType::GetMemberName(arguments[0]->return_type, i));
		FlatVector::GetData<string_t>(varchar_vector)[i] =
		    str.IsInlined() ? str : StringVector::AddString(varchar_vector, str);
	}
	auto enum_type = LogicalType::ENUM("", varchar_vector, member_count);
	bound_function.return_type = enum_type;

	return nullptr;
}

static void UnionTagFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::ENUM);
	auto &union_vector = args.data[0];
	auto enums = FlatVector::GetData<uint8_t>(result);
	auto tags = UnionVector::GetTags(union_vector);

	UnifiedVectorFormat sdata;
	args.data[0].ToUnifiedFormat(args.size(), sdata);

	for (idx_t i = 0; i < args.size(); i++) {
		auto idx = sdata.sel->get_index(i);
		auto tag = tags[idx];
		if (sdata.validity.RowIsValid(idx)) {
			enums[i] = tag;
		} else {
			FlatVector::SetNull(result, i, true);
		}
	}

	if (args.size() == 1) {
		result.Flatten(args.size());
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

void UnionTagFun::RegisterFunction(BuiltinFunctions &set) {
	auto fun = ScalarFunction("union_tag", {LogicalTypeId::UNION}, LogicalTypeId::ANY, UnionTagFunction, UnionTagBind,
	                          nullptr, nullptr); // TODO: Statistics?

	ScalarFunctionSet union_tag("union_tag");
	union_tag.AddFunction(fun);
	set.AddFunction(union_tag);
}

} // namespace duckdb
