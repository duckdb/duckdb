#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/struct_functions.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/function/scalar/struct_utils.hpp"

namespace duckdb {

struct RemapColumnInfo {
	RemapColumnInfo(idx_t index, bool is_default) : index(index), is_default(is_default) {
	}

	idx_t index;
	bool is_default;

	inline bool operator==(const RemapColumnInfo &rhs) const {
		return index == rhs.index && is_default == rhs.is_default;
	};
};

struct RemapStructBindData : public FunctionData {
	explicit RemapStructBindData(vector<RemapColumnInfo> remap_info_p) : remap_info(std::move(remap_info_p)) {
	}

	vector<RemapColumnInfo> remap_info;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<RemapStructBindData>(remap_info);
	};

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<RemapStructBindData>();
		return remap_info == other.remap_info;
	}
};

static void RemapStructFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<RemapStructBindData>();

	auto &input = args.data[0];
	auto &input_vectors = StructVector::GetEntries(input);
	auto &result_vectors = StructVector::GetEntries(result);

	if (result_vectors.size() != info.remap_info.size()) {
		throw InternalException("Remap info unaligned in remap struct");
	}
	bool has_top_level_null = false;
	UnifiedVectorFormat format;
	// copy over the NULL values from the input vector
	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (ConstantVector::IsNull(input)) {
			ConstantVector::SetNull(result, true);
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			return;
		}
	} else {
		auto &validity = FlatVector::Validity(input);
		if (!validity.AllValid()) {
			FlatVector::SetValidity(result, validity);
			has_top_level_null = true;
		}
	}
	// set up the correct vector references
	for (idx_t i = 0; i < info.remap_info.size(); i++) {
		auto &remap = info.remap_info[i];
		if (remap.is_default) {
			auto &defaults = StructVector::GetEntries(args.data[3]);
			result_vectors[i]->Reference(*defaults[remap.index]);
			if (result_vectors[i]->GetVectorType() != VectorType::CONSTANT_VECTOR) {
				throw InternalException("Default value in remap struct must be a constant");
			}
			if (has_top_level_null && !ConstantVector::IsNull(*result_vectors[i])) {
				// if we have any top-level NULL values and the default value is not NULL, we need to propagate the NULL
				// values to the default value
				result_vectors[i]->Flatten(args.size());
				FlatVector::SetValidity(*result_vectors[i], FlatVector::Validity(result));
			}
		} else {
			result_vectors[i]->Reference(*input_vectors[remap.index]);
		}
	}
	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	result.Verify(args.size());
}

static unique_ptr<FunctionData> RemapStructBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {
	for (idx_t arg_idx = 0; arg_idx < arguments.size(); arg_idx++) {
		auto &arg = arguments[arg_idx];
		if (arg->return_type.id() == LogicalTypeId::UNKNOWN) {
			throw ParameterNotResolvedException();
		}
		if (arg->return_type.id() != LogicalTypeId::STRUCT) {
			if (arg_idx == 3 && arg->return_type.id() == LogicalTypeId::SQLNULL) {
				// defaults can be NULL
			} else {
				throw BinderException("Struct remap can only remap structs");
			}
		} else if (StructType::IsUnnamed(arg->return_type)) {
			throw BinderException("Struct remap can only remap named structs");
		}
	}
	auto &from_type = arguments[0]->return_type;
	auto &to_type = arguments[1]->return_type;
	auto &source_children = StructType::GetChildTypes(from_type);
	auto &target_children = StructType::GetChildTypes(to_type);
	if (target_children.empty()) {
		throw InternalException("Empty struct");
	}
	if (!arguments[2]->IsFoldable()) {
		throw BinderException("Remap keys for remap_struct needs to be a constant value");
	}
	case_insensitive_map_t<idx_t> source_map;
	for (idx_t source_idx = 0; source_idx < source_children.size(); source_idx++) {
		source_map.emplace(source_children[source_idx].first, source_idx);
	}
	case_insensitive_map_t<idx_t> target_map;
	for (idx_t target_idx = 0; target_idx < target_children.size(); target_idx++) {
		target_map.emplace(target_children[target_idx].first, target_idx);
	}
	case_insensitive_map_t<RemapColumnInfo> remap_map;

	Value remap_val = ExpressionExecutor::EvaluateScalar(context, *arguments[2]);
	auto &remap_types = StructType::GetChildTypes(arguments[2]->return_type);
	auto &remap_values = StructValue::GetChildren(remap_val);

	case_insensitive_map_t<LogicalType> source_type_remap;
	for (idx_t remap_idx = 0; remap_idx < remap_values.size(); remap_idx++) {
		auto &remap_val = remap_values[remap_idx];
		if (remap_val.type().id() != LogicalTypeId::VARCHAR) {
			throw BinderException("Remap keys for remap_struct needs to be a string");
		}
		auto remap_source = remap_val.ToString();
		auto &remap_target = remap_types[remap_idx].first;

		// find the source index
		auto entry = source_map.find(remap_source);
		if (entry == source_map.end()) {
			throw BinderException("Source value %s not found", remap_source);
		}
		auto target_entry = target_map.find(remap_target);
		if (target_entry == target_map.end()) {
			throw BinderException("Target value %s not found", remap_target);
		}
		source_type_remap.emplace(remap_source, target_children[target_entry->second].second);

		RemapColumnInfo remap(entry->second, false);
		remap_map.emplace(remap_target, remap);
	}
	if (!arguments[3]->IsFoldable()) {
		throw BinderException("Default values must be constants");
	}

	if (arguments[3]->return_type.id() != LogicalTypeId::SQLNULL) {
		auto &default_types = StructType::GetChildTypes(arguments[3]->return_type);
		for (idx_t default_idx = 0; default_idx < default_types.size(); default_idx++) {
			auto &default_target = default_types[default_idx].first;
			auto &default_type = default_types[default_idx].second;
			auto entry = target_map.find(default_target);
			if (entry == target_map.end()) {
				throw BinderException("Default value %s not found for remap", default_target);
			}
			auto &target_type = target_children[entry->second].second;
			if (default_type != target_type) {
				throw BinderException("Default key %s does not match target type %s - add a cast", default_target,
				                      target_type);
			}

			RemapColumnInfo remap(default_idx, true);
			auto added = remap_map.emplace(default_target, remap);
			if (!added.second) {
				throw BinderException("Duplicate value provided for target %s", default_target);
			}
		}
	}

	// construct the final remapping
	vector<RemapColumnInfo> remap;
	for (idx_t target_idx = 0; target_idx < target_children.size(); target_idx++) {
		auto &target_name = target_children[target_idx].first;
		auto entry = remap_map.find(target_name);
		if (entry == remap_map.end()) {
			throw BinderException("Missing target value %s for remap", target_name);
		}
		remap.push_back(entry->second);
	}
	// push a cast for argument 0 to match up the source types to the target
	child_list_t<LogicalType> new_source_children;
	for (idx_t source_idx = 0; source_idx < source_children.size(); source_idx++) {
		auto &child_name = source_children[source_idx].first;
		auto entry = source_type_remap.find(child_name);
		if (entry != source_type_remap.end()) {
			// this entry is remapped - fetch the target type
			new_source_children.emplace_back(child_name, entry->second);
		} else {
			// entry is not remapped - keep the original type
			new_source_children.push_back(source_children[source_idx]);
		}
	}

	bound_function.arguments[0] = LogicalType::STRUCT(std::move(new_source_children));
	bound_function.arguments[1] = arguments[1]->return_type;
	bound_function.arguments[2] = arguments[2]->return_type;
	bound_function.arguments[3] = arguments[3]->return_type;
	bound_function.return_type = arguments[1]->return_type;

	return make_uniq<RemapStructBindData>(std::move(remap));
}

ScalarFunction RemapStructFun::GetFunction() {
	ScalarFunction remap("remap_struct",
	                     {LogicalTypeId::STRUCT, LogicalTypeId::STRUCT, LogicalTypeId::STRUCT, LogicalTypeId::STRUCT},
	                     LogicalTypeId::STRUCT, RemapStructFunction, RemapStructBind);
	remap.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return remap;
}

} // namespace duckdb
