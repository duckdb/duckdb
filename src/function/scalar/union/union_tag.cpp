#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"

namespace duckdb {

struct UnionTagBindData : public FunctionData {
	UnionTagBindData() {
	}

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_unique<UnionTagBindData>();
	}
	bool Equals(const FunctionData &other_p) const override {
		return true;
	}
};

static void UnionTagFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	auto &union_vector = args.data[0];
	union_vector.Verify(args.size());
	auto types = UnionType::GetMemberTypes(union_vector.GetType());

	// Create a string vector with the name of each tag
	auto tag_name_vector = Vector(LogicalType::VARCHAR, types.size() + 1);
	auto tag_name_vector_entries = FlatVector::GetData<string_t>(tag_name_vector);
	for (idx_t tag_idx = 0; tag_idx < types.size(); tag_idx++) {
		auto tag_str = string_t(types[tag_idx].first);

		tag_name_vector_entries[tag_idx] = tag_str.IsInlined() 
			? tag_str 
			: StringVector::AddString(tag_name_vector, tag_str);
	}
	// add a null tag entry for null union values
	FlatVector::SetNull(tag_name_vector, types.size(), true);

	// map the entries tags to the actual tag names using a selection vector
	auto tags = UnionVector::GetTags(union_vector);

	UnifiedVectorFormat sdata;
	args.data[0].ToUnifiedFormat(args.size(), sdata);

	SelectionVector selection(args.size());
	for (idx_t i = 0; i < args.size(); i++) {
		auto idx = sdata.sel->get_index(i);
		
		auto tag = tags[idx];
		if (sdata.validity.RowIsValid(idx)) {
			selection.set_index(i, tag);	
		} else {
			// point to the null tag entry if the union itself is null
			selection.set_index(i, types.size());
		}
	}
	result.Slice(tag_name_vector, selection, args.size());
	result.Verify(args.size());
}

static unique_ptr<FunctionData> UnionTagBind(ClientContext &context, ScalarFunction &bound_function,
                                             vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 1);
	if (arguments[0]->return_type.id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}
	D_ASSERT(LogicalTypeId::UNION == arguments[0]->return_type.id());

	auto &union_children = UnionType::GetMemberTypes(arguments[0]->return_type);
	if (union_children.empty()) {
		throw InternalException("Can't get tags from an empty union");
	}
	bound_function.arguments[0] = arguments[0]->return_type;

	return make_unique<UnionTagBindData>();
}

void UnionTagFun::RegisterFunction(BuiltinFunctions &set) {
	auto fun = ScalarFunction("union_tag", {LogicalTypeId::UNION}, LogicalType::VARCHAR, UnionTagFunction, UnionTagBind,
	                      nullptr, nullptr); // TODO: Statistics?

	ScalarFunctionSet union_tag("union_tag");
	union_tag.AddFunction(fun);
	set.AddFunction(union_tag);
}

} // namespace duckdb
