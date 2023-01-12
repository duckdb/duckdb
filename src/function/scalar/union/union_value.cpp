#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"

namespace duckdb {

UnionInvalidReason CheckUnionValidity(Vector &vector, idx_t count, const SelectionVector &sel) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::UNION);
	auto member_count = UnionType::GetMemberCount(vector.GetType());
	if (member_count == 0) {
		return UnionInvalidReason::NO_MEMBERS;
	}

	UnifiedVectorFormat union_vdata;
	vector.ToUnifiedFormat(count, union_vdata);

	UnifiedVectorFormat tags_vdata;
	auto &tag_vector = UnionVector::GetTags(vector);
	tag_vector.ToUnifiedFormat(count, tags_vdata);

	// check that only one member is valid at a time
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto union_mapped_row_idx = sel.get_index(row_idx);
		if (!union_vdata.validity.RowIsValid(union_mapped_row_idx)) {
			continue;
		}

		auto tag_mapped_row_idx = tags_vdata.sel->get_index(row_idx);
		if (!tags_vdata.validity.RowIsValid(tag_mapped_row_idx)) {
			continue;
		}

		auto tag = ((union_tag_t *)tags_vdata.data)[tag_mapped_row_idx];
		if (tag >= member_count) {
			return UnionInvalidReason::TAG_OUT_OF_RANGE;
		}

		bool found_valid = false;
		for (idx_t member_idx = 0; member_idx < member_count; member_idx++) {

			UnifiedVectorFormat member_vdata;
			auto &member = UnionVector::GetMember(vector, member_idx);
			member.ToUnifiedFormat(count, member_vdata);

			auto mapped_row_idx = member_vdata.sel->get_index(row_idx);
			if (member_vdata.validity.RowIsValid(mapped_row_idx)) {
				if (found_valid) {
					return UnionInvalidReason::VALIDITY_OVERLAP;
				}
				found_valid = true;
			}
		}
	}

	return UnionInvalidReason::VALID;
}

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

	// Set the result tag vector to a constant value
	auto &tag_vector = UnionVector::GetTags(result);
	tag_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::GetData<union_tag_t>(tag_vector)[0] = 0;

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	result.Verify(args.size());
}

static unique_ptr<FunctionData> UnionValueBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {

	if (arguments.size() != 1) {
		throw BinderException("union_value takes exactly one argument");
	}
	auto &child = arguments[0];

	if (child->alias.empty()) {
		throw BinderException("Need named argument for union tag, e.g. UNION_VALUE(a := b)");
	}

	child_list_t<LogicalType> union_members;

	union_members.push_back(make_pair(child->alias, child->return_type));

	bound_function.return_type = LogicalType::UNION(std::move(union_members));
	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

void UnionValueFun::RegisterFunction(BuiltinFunctions &set) {

	auto fun =
	    ScalarFunction("union_value", {}, LogicalTypeId::UNION, UnionValueFunction, UnionValueBind, nullptr, nullptr);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	fun.serialize = VariableReturnBindData::Serialize;
	fun.deserialize = VariableReturnBindData::Deserialize;

	ScalarFunctionSet union_value("union_value");
	union_value.AddFunction(fun);
	set.AddFunction(union_value);
}

} // namespace duckdb
