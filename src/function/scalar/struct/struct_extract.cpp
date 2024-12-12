#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/struct_functions.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/function/scalar/struct_utils.hpp"

namespace duckdb {

static void StructExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<StructExtractBindData>();

	// this should be guaranteed by the binder
	auto &vec = args.data[0];

	vec.Verify(args.size());
	auto &children = StructVector::GetEntries(vec);
	D_ASSERT(info.index < children.size());
	auto &struct_child = children[info.index];
	result.Reference(*struct_child);
	result.Verify(args.size());
}

static unique_ptr<FunctionData> StructExtractBind(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2);
	auto &child_type = arguments[0]->return_type;
	if (child_type.id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}
	D_ASSERT(LogicalTypeId::STRUCT == child_type.id());
	auto &struct_children = StructType::GetChildTypes(child_type);
	if (struct_children.empty()) {
		throw InternalException("Can't extract something from an empty struct");
	}
	if (StructType::IsUnnamed(child_type)) {
		throw BinderException(
		    "struct_extract with a string key cannot be used on an unnamed struct, use a numeric index instead");
	}
	bound_function.arguments[0] = child_type;

	auto &key_child = arguments[1];
	if (key_child->HasParameter()) {
		throw ParameterNotResolvedException();
	}

	if (key_child->return_type.id() != LogicalTypeId::VARCHAR || !key_child->IsFoldable()) {
		throw BinderException("Key name for struct_extract needs to be a constant string");
	}
	Value key_val = ExpressionExecutor::EvaluateScalar(context, *key_child);
	D_ASSERT(key_val.type().id() == LogicalTypeId::VARCHAR);
	auto &key_str = StringValue::Get(key_val);
	if (key_val.IsNull() || key_str.empty()) {
		throw BinderException("Key name for struct_extract needs to be neither NULL nor empty");
	}
	string key = StringUtil::Lower(key_str);

	LogicalType return_type;
	idx_t key_index = 0;
	bool found_key = false;

	for (size_t i = 0; i < struct_children.size(); i++) {
		auto &child = struct_children[i];
		if (StringUtil::Lower(child.first) == key) {
			found_key = true;
			key_index = i;
			return_type = child.second;
			break;
		}
	}

	if (!found_key) {
		vector<string> candidates;
		candidates.reserve(struct_children.size());
		for (auto &struct_child : struct_children) {
			candidates.push_back(struct_child.first);
		}
		auto closest_settings = StringUtil::TopNJaroWinkler(candidates, key);
		auto message = StringUtil::CandidatesMessage(closest_settings, "Candidate Entries");
		throw BinderException("Could not find key \"%s\" in struct\n%s", key, message);
	}

	bound_function.return_type = std::move(return_type);
	return GetBindData(key_index);
}

static unique_ptr<FunctionData> StructExtractBindInternal(ClientContext &context, ScalarFunction &bound_function,
                                                          vector<unique_ptr<Expression>> &arguments,
                                                          bool struct_extract) {
	D_ASSERT(bound_function.arguments.size() == 2);
	auto &child_type = arguments[0]->return_type;
	if (child_type.id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}
	D_ASSERT(LogicalTypeId::STRUCT == child_type.id());
	auto &struct_children = StructType::GetChildTypes(child_type);
	if (struct_children.empty()) {
		throw InternalException("Can't extract something from an empty struct");
	}
	if (struct_extract && !StructType::IsUnnamed(child_type)) {
		throw BinderException(
		    "struct_extract with an integer key can only be used on unnamed structs, use a string key instead");
	}
	bound_function.arguments[0] = child_type;

	auto &key_child = arguments[1];
	if (key_child->HasParameter()) {
		throw ParameterNotResolvedException();
	}

	if (!key_child->IsFoldable()) {
		throw BinderException("Key index for struct_extract needs to be a constant value");
	}
	Value key_val = ExpressionExecutor::EvaluateScalar(context, *key_child);
	auto index = key_val.GetValue<int64_t>();
	if (index <= 0 || idx_t(index) > struct_children.size()) {
		throw BinderException("Key index %lld for struct_extract out of range - expected an index between 1 and %llu",
		                      index, struct_children.size());
	}
	bound_function.return_type = struct_children[NumericCast<idx_t>(index - 1)].second;
	return GetBindData(NumericCast<idx_t>(index - 1));
}

static unique_ptr<FunctionData> StructExtractBindIndex(ClientContext &context, ScalarFunction &bound_function,
                                                       vector<unique_ptr<Expression>> &arguments) {
	return StructExtractBindInternal(context, bound_function, arguments, true);
}

static unique_ptr<FunctionData> StructExtractAtBind(ClientContext &context, ScalarFunction &bound_function,
                                                    vector<unique_ptr<Expression>> &arguments) {
	return StructExtractBindInternal(context, bound_function, arguments, false);
}

static unique_ptr<BaseStatistics> PropagateStructExtractStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &bind_data = input.bind_data;

	auto &info = bind_data->Cast<StructExtractBindData>();
	auto struct_child_stats = StructStats::GetChildStats(child_stats[0]);
	return struct_child_stats[info.index].ToUnique();
}

unique_ptr<FunctionData> GetBindData(idx_t index) {
	return make_uniq<StructExtractBindData>(index);
}

ScalarFunction GetKeyExtractFunction() {
	return ScalarFunction("struct_extract", {LogicalTypeId::STRUCT, LogicalType::VARCHAR}, LogicalType::ANY,
	                      StructExtractFunction, StructExtractBind, nullptr, PropagateStructExtractStats);
}

ScalarFunction GetIndexExtractFunction() {
	return ScalarFunction("struct_extract", {LogicalTypeId::STRUCT, LogicalType::BIGINT}, LogicalType::ANY,
	                      StructExtractFunction, StructExtractBindIndex);
}

ScalarFunction GetExtractAtFunction() {
	return ScalarFunction("struct_extract_at", {LogicalTypeId::STRUCT, LogicalType::BIGINT}, LogicalType::ANY,
	                      StructExtractFunction, StructExtractAtBind);
}

ScalarFunctionSet StructExtractFun::GetFunctions() {
	// the arguments and return types are actually set in the binder function
	ScalarFunctionSet struct_extract_set("struct_extract");
	struct_extract_set.AddFunction(GetKeyExtractFunction());
	struct_extract_set.AddFunction(GetIndexExtractFunction());
	return struct_extract_set;
}

ScalarFunctionSet StructExtractAtFun::GetFunctions() {
	ScalarFunctionSet struct_extractat_set("struct_extract_at");
	struct_extractat_set.AddFunction(GetExtractAtFunction());
	return struct_extractat_set;
}

} // namespace duckdb
