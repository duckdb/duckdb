#include "core_functions/scalar/union_functions.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"

namespace duckdb {

struct UnionExtractBindData : public FunctionData {
	UnionExtractBindData(string key, idx_t index, LogicalType type)
	    : key(std::move(key)), index(index), type(std::move(type)) {
	}

	string key;
	idx_t index;
	LogicalType type;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<UnionExtractBindData>(key, index, type);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<UnionExtractBindData>();
		return key == other.key && index == other.index && type == other.type;
	}
};

static void UnionExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<UnionExtractBindData>();

	// this should be guaranteed by the binder
	auto &vec = args.data[0];
	vec.Verify(args.size());

	D_ASSERT(info.index < UnionType::GetMemberCount(vec.GetType()));
	auto &member = UnionVector::GetMember(vec, info.index);
	result.Reference(member);
	result.Verify(args.size());
}

static unique_ptr<FunctionData> UnionExtractBind(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2);
	if (arguments[0]->return_type.id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}
	if (arguments[0]->return_type.id() != LogicalTypeId::UNION) {
		throw BinderException("union_extract can only take a union parameter");
	}
	idx_t union_member_count = UnionType::GetMemberCount(arguments[0]->return_type);
	if (union_member_count == 0) {
		throw InternalException("Can't extract something from an empty union");
	}
	bound_function.arguments[0] = arguments[0]->return_type;

	auto &key_child = arguments[1];
	if (key_child->HasParameter()) {
		throw ParameterNotResolvedException();
	}

	if (key_child->return_type.id() != LogicalTypeId::VARCHAR || !key_child->IsFoldable()) {
		throw BinderException("Key name for union_extract needs to be a constant string");
	}
	Value key_val = ExpressionExecutor::EvaluateScalar(context, *key_child);
	D_ASSERT(key_val.type().id() == LogicalTypeId::VARCHAR);
	auto &key_str = StringValue::Get(key_val);
	if (key_val.IsNull() || key_str.empty()) {
		throw BinderException("Key name for union_extract needs to be neither NULL nor empty");
	}
	string key = StringUtil::Lower(key_str);

	LogicalType return_type;
	idx_t key_index = 0;
	bool found_key = false;

	for (size_t i = 0; i < union_member_count; i++) {
		auto &member_name = UnionType::GetMemberName(arguments[0]->return_type, i);
		if (StringUtil::Lower(member_name) == key) {
			found_key = true;
			key_index = i;
			return_type = UnionType::GetMemberType(arguments[0]->return_type, i);
			break;
		}
	}

	if (!found_key) {
		vector<string> candidates;
		candidates.reserve(union_member_count);
		for (idx_t i = 0; i < union_member_count; i++) {
			candidates.push_back(UnionType::GetMemberName(arguments[0]->return_type, i));
		}
		auto closest_settings = StringUtil::TopNJaroWinkler(candidates, key);
		auto message = StringUtil::CandidatesMessage(closest_settings, "Candidate Entries");
		throw BinderException("Could not find key \"%s\" in union\n%s", key, message);
	}

	bound_function.return_type = return_type;
	return make_uniq<UnionExtractBindData>(key, key_index, return_type);
}

ScalarFunction UnionExtractFun::GetFunction() {
	// the arguments and return types are actually set in the binder function
	return ScalarFunction({LogicalTypeId::UNION, LogicalType::VARCHAR}, LogicalType::ANY, UnionExtractFunction,
	                      UnionExtractBind, nullptr, nullptr);
}

} // namespace duckdb
