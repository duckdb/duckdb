#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/struct_functions.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/function/scalar/struct_utils.hpp"

namespace duckdb {

namespace {

static bool IsRemappable(const LogicalType &type) {
	return type.IsNested() && type.id() != LogicalTypeId::VARIANT;
}

struct RemapColumnInfo {
	optional_idx index;
	optional_idx default_index;
	vector<RemapColumnInfo> child_remap_info;

	inline bool operator==(const RemapColumnInfo &rhs) const {
		return index == rhs.index && default_index == rhs.default_index && child_remap_info == rhs.child_remap_info;
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

void RemapNested(Vector &input, Vector &default_vector, Vector &result, idx_t result_size,
                 const vector<RemapColumnInfo> &remap_info);

void RemapChildVectors(const Vector &result, const vector<reference<Vector>> &input_vectors,
                       const vector<reference<Vector>> &result_vectors, const vector<RemapColumnInfo> &remap_info,
                       Vector &default_vector, const bool has_top_level_null, idx_t count) {
	// set up the correct vector references
	for (idx_t i = 0; i < remap_info.size(); i++) {
		auto &remap = remap_info[i];
		if (remap.index.IsValid() && !remap.child_remap_info.empty()) {
			// nested remap - recurse
			auto &input_vector = input_vectors[remap.index.GetIndex()];
			reference<Vector> child_default = default_vector;
			if (remap.default_index.IsValid()) {
				auto &defaults = StructVector::GetEntries(default_vector);
				child_default = *defaults[remap.default_index.GetIndex()];
			}
			RemapNested(input_vector, child_default.get(), result_vectors[i], count, remap.child_remap_info);
			continue;
		}
		// primitive type remap
		if (remap.default_index.IsValid()) {
			auto &defaults = StructVector::GetEntries(default_vector);
			result_vectors[i].get().Reference(*defaults[remap.default_index.GetIndex()]);
			if (result_vectors[i].get().GetVectorType() != VectorType::CONSTANT_VECTOR) {
				throw InternalException("Default value in remap struct must be a constant");
			}
			if (has_top_level_null && !ConstantVector::IsNull(result_vectors[i])) {
				// if we have any top-level NULL values and the default value is not NULL, we need to propagate the NULL
				// values to the default value
				result_vectors[i].get().Flatten(count);
				FlatVector::SetValidity(result_vectors[i], FlatVector::Validity(result));
			}
		} else {
			result_vectors[i].get().Reference(input_vectors[remap.index.GetIndex()]);
		}
	}
}

void RemapMap(Vector &input, Vector &default_vector, Vector &result, idx_t result_size,
              const vector<RemapColumnInfo> &remap_info) {
	auto &input_key_vector = MapVector::GetKeys(input);
	auto &input_value_vector = MapVector::GetValues(input);

	auto &result_key_vector = MapVector::GetKeys(result);
	auto &result_value_vector = MapVector::GetValues(result);
	auto list_size = ListVector::GetListSize(input);
	ListVector::Reserve(result, list_size);
	ListVector::SetListSize(result, list_size);

	bool has_top_level_null = false;
	// copy over the NULL values from the input vector
	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (ConstantVector::IsNull(input)) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
			return;
		}
		auto list_data = FlatVector::GetData<list_entry_t>(input);
		auto result_list_data = FlatVector::GetData<list_entry_t>(result);
		memcpy(result_list_data, list_data, sizeof(list_entry_t));
	} else {
		UnifiedVectorFormat format;
		input.ToUnifiedFormat(result_size, format);
		if (!format.validity.AllValid()) {
			auto &result_validity = FlatVector::Validity(result);
			for (idx_t i = 0; i < result_size; i++) {
				auto input_idx = format.sel->get_index(i);
				if (!format.validity.RowIsValid(input_idx)) {
					result_validity.SetInvalid(i);
				}
			}
			has_top_level_null = !result_validity.AllValid();
		}
		auto list_data = UnifiedVectorFormat::GetData<list_entry_t>(format);
		auto result_list_data = FlatVector::GetData<list_entry_t>(result);
		for (idx_t i = 0; i < result_size; i++) {
			result_list_data[i] = list_data[format.sel->get_index(i)];
		}
	}
	// set up the correct vector references
	D_ASSERT(remap_info.size() == 2);

	//! Build up the inputs for remapping the children of the map
	vector<reference<Vector>> input_vectors;
	input_vectors.emplace_back(input_key_vector);
	input_vectors.emplace_back(input_value_vector);

	vector<reference<Vector>> result_vectors;
	result_vectors.emplace_back(result_key_vector);
	result_vectors.emplace_back(result_value_vector);

	RemapChildVectors(result, input_vectors, result_vectors, remap_info, default_vector, has_top_level_null, list_size);
}

void RemapList(Vector &input, Vector &default_vector, Vector &result, idx_t result_size,
               const vector<RemapColumnInfo> &remap_info) {
	auto &input_vector = ListVector::GetEntry(input);
	auto &result_vector = ListVector::GetEntry(result);
	auto list_size = ListVector::GetListSize(input);
	ListVector::Reserve(result, list_size);
	ListVector::SetListSize(result, list_size);

	bool has_top_level_null = false;
	// copy over the NULL values from the input vector
	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (ConstantVector::IsNull(input)) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
			return;
		}
		auto list_data = FlatVector::GetData<list_entry_t>(input);
		auto result_list_data = FlatVector::GetData<list_entry_t>(result);
		memcpy(result_list_data, list_data, sizeof(list_entry_t));
	} else {
		UnifiedVectorFormat format;
		input.ToUnifiedFormat(result_size, format);
		if (!format.validity.AllValid()) {
			auto &result_validity = FlatVector::Validity(result);
			for (idx_t i = 0; i < result_size; i++) {
				auto input_idx = format.sel->get_index(i);
				if (!format.validity.RowIsValid(input_idx)) {
					result_validity.SetInvalid(i);
				}
			}
			has_top_level_null = !result_validity.AllValid();
		}
		auto list_data = UnifiedVectorFormat::GetData<list_entry_t>(format);
		auto result_list_data = FlatVector::GetData<list_entry_t>(result);
		for (idx_t i = 0; i < result_size; i++) {
			result_list_data[i] = list_data[format.sel->get_index(i)];
		}
	}

	//! Build up the input for remapping the child of the list
	vector<reference<Vector>> input_vectors;
	input_vectors.emplace_back(input_vector);

	vector<reference<Vector>> result_vectors;
	result_vectors.emplace_back(result_vector);

	RemapChildVectors(result, input_vectors, result_vectors, remap_info, default_vector, has_top_level_null, list_size);
}

void RemapStruct(Vector &input, Vector &default_vector, Vector &result, idx_t result_size,
                 const vector<RemapColumnInfo> &remap_info) {
	auto &input_child_vectors = StructVector::GetEntries(input);
	auto &result_child_vectors = StructVector::GetEntries(result);
	if (result_child_vectors.size() != remap_info.size()) {
		throw InternalException("Remap info unaligned in remap struct");
	}
	bool has_top_level_null = false;
	// copy over the NULL values from the input vector
	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (ConstantVector::IsNull(input)) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
			return;
		}
	} else {
		UnifiedVectorFormat format;
		input.ToUnifiedFormat(result_size, format);
		if (!format.validity.AllValid()) {
			auto &result_validity = FlatVector::Validity(result);
			for (idx_t i = 0; i < result_size; i++) {
				auto input_idx = format.sel->get_index(i);
				if (!format.validity.RowIsValid(input_idx)) {
					result_validity.SetInvalid(i);
				}
			}
			has_top_level_null = !result_validity.AllValid();
		}
	}

	//! Build up the input for remapping the children of the struct
	vector<reference<Vector>> input_vectors;
	for (auto &child : input_child_vectors) {
		input_vectors.emplace_back(*child);
	}

	vector<reference<Vector>> result_vectors;
	for (auto &child : result_child_vectors) {
		result_vectors.emplace_back(*child);
	}

	RemapChildVectors(result, input_vectors, result_vectors, remap_info, default_vector, has_top_level_null,
	                  result_size);
}

void RemapNested(Vector &input, Vector &default_vector, Vector &result, idx_t result_size,
                 const vector<RemapColumnInfo> &remap_info) {
	auto &source_type = input.GetType();
	D_ASSERT(IsRemappable(source_type));
	switch (source_type.id()) {
	case LogicalTypeId::STRUCT:
		return RemapStruct(input, default_vector, result, result_size, remap_info);
	case LogicalTypeId::LIST:
		return RemapList(input, default_vector, result, result_size, remap_info);
	case LogicalTypeId::MAP:
		return RemapMap(input, default_vector, result, result_size, remap_info);
	default:
		throw InvalidInputException("Can't RemapNested for type '%s'", source_type.ToString());
	}
}

void RemapStructFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<RemapStructBindData>();

	auto &input = args.data[0];

	RemapNested(input, args.data[3], result, args.size(), info.remap_info);
	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	result.Verify(args.size());
}
struct RemapIndex {
	idx_t index;
	LogicalType type;
	unique_ptr<case_insensitive_map_t<RemapIndex>> child_map;

	static case_insensitive_map_t<RemapIndex> GetMap(const LogicalType &type) {
		case_insensitive_map_t<RemapIndex> result;
		switch (type.id()) {
		case LogicalTypeId::STRUCT: {
			auto &children = StructType::GetChildTypes(type);
			for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
				auto &child = children[child_idx];
				result.emplace(child.first, GetIndex(child_idx, child.second));
			}
			break;
		}
		case LogicalTypeId::LIST: {
			auto &child = ListType::GetChildType(type);
			result.emplace("list", GetIndex(0, child));
			break;
		}
		case LogicalTypeId::MAP: {
			auto &key = MapType::KeyType(type);
			auto &value = MapType::ValueType(type);
			result.emplace("key", GetIndex(0, key));
			result.emplace("value", GetIndex(1, value));
			break;
		}
		default:
			throw BinderException("Can't remap type %s", type.ToString());
		}
		return result;
	}

	static RemapIndex GetIndex(idx_t idx, const LogicalType &type) {
		RemapIndex index;
		index.index = idx;
		index.type = type;
		if (IsRemappable(type)) {
			index.child_map = make_uniq<case_insensitive_map_t<RemapIndex>>(GetMap(type));
		}
		return index;
	}
};

struct RemapEntry {
	optional_idx index;
	optional_idx default_index;
	LogicalType target_type;
	unique_ptr<case_insensitive_map_t<RemapEntry>> child_remaps;

	static void PerformRemap(const string &remap_target, const Value &remap_val,
	                         case_insensitive_map_t<RemapIndex> &source_map,
	                         case_insensitive_map_t<RemapIndex> &target_map, case_insensitive_map_t<RemapEntry> &result,
	                         const LogicalType &parent_type) {
		string remap_source;
		Value struct_val;
		if (remap_val.type().id() == LogicalTypeId::VARCHAR) {
			remap_source = remap_val.ToString();
		} else if (remap_val.type().id() == LogicalTypeId::STRUCT) {
			if (!StructType::IsUnnamed(remap_val.type())) {
				throw BinderException("Remap keys for remap_struct needs to be an unnamed struct");
			}
			auto &children = StructValue::GetChildren(remap_val);
			if (children.size() != 2) {
				throw BinderException("Remap keys for remap_struct needs to have two children");
			}
			if (children[0].type().id() != LogicalTypeId::VARCHAR || children[1].type().id() != LogicalTypeId::STRUCT) {
				throw BinderException("Remap keys for remap_struct need to be varchar and struct");
			}
			remap_source = children[0].ToString();
			struct_val = children[1];
		} else {
			throw BinderException("Remap keys for remap_struct needs to be a string or struct");
		}

		// find the source index
		auto entry = source_map.find(remap_source);
		if (entry == source_map.end()) {
			throw BinderException("Source value %s not found", remap_source);
		}
		auto target_entry = target_map.find(remap_target);
		if (target_entry == target_map.end()) {
			throw BinderException("Target value %s not found", remap_target);
		}

		auto &source_type = entry->second.type;
		auto &target_type = target_entry->second.type;

		bool source_is_nested = IsRemappable(source_type);
		bool target_is_nested = IsRemappable(target_type);
		RemapEntry remap;
		remap.index = entry->second.index;
		remap.target_type = target_entry->second.type;
		if (source_is_nested || target_is_nested || !struct_val.IsNull()) {
			if (source_type.id() != target_type.id()) {
				throw BinderException("Can't change source type (%s) to target type (%s), type conversion not allowed",
				                      source_type.ToString(), target_type.ToString());
			}
			if (!struct_val.IsNull()) {
				// this is a struct - we actually need all 3 of these to be true (or none of them to be true)
				if (!source_is_nested || !target_is_nested || struct_val.IsNull()) {
					throw BinderException("Found a struct value (%s) as a remap, this is only expected for a nested "
					                      "type, source type is '%s', target type is '%s'",
					                      struct_val.ToString(), entry->second.type.ToString(),
					                      target_entry->second.type.ToString());
				}
				remap.child_remaps = make_uniq<case_insensitive_map_t<RemapEntry>>();
				auto &remap_types = StructType::GetChildTypes(struct_val.type());
				auto &remap_values = StructValue::GetChildren(struct_val);
				for (idx_t child_idx = 0; child_idx < remap_types.size(); child_idx++) {
					PerformRemap(remap_types[child_idx].first, remap_values[child_idx], *entry->second.child_map,
					             *target_entry->second.child_map, *remap.child_remaps, source_type);
				}
			}
		}
		result.emplace(remap_target, std::move(remap));
	}

	static void HandleDefault(idx_t default_idx, const string &default_target, const LogicalType &default_type,
	                          case_insensitive_map_t<RemapIndex> &target_map,
	                          case_insensitive_map_t<RemapEntry> &result) {
		auto entry = target_map.find(default_target);
		if (entry == target_map.end()) {
			throw BinderException("Default value %s not found for remap", default_target);
		}
		auto &target_type = entry->second.type;

		RemapEntry remap;
		remap.default_index = default_idx;
		if (default_type.id() == LogicalTypeId::STRUCT) {
			// nested remap - recurse
			if (!IsRemappable(target_type)) {
				throw BinderException("Default value is a struct - target value should be a nested type, is '%s'",
				                      target_type.ToString());
			}
			// add to the map at this level only if it does not yet exist
			auto result_entry = result.find(default_target);
			if (result_entry == result.end()) {
				result.emplace(default_target, std::move(remap));
				result_entry = result.find(default_target);
				result_entry->second.child_remaps = make_uniq<case_insensitive_map_t<RemapEntry>>();
			} else {
				// the entry exists - add the default index
				result_entry->second.default_index = default_idx;
			}
			auto &child_types = StructType::GetChildTypes(default_type);
			for (idx_t child_idx = 0; child_idx < child_types.size(); child_idx++) {
				auto &child_default = child_types[child_idx];
				if (!result_entry->second.child_remaps || !entry->second.child_map) {
					throw BinderException("No child remaps found");
				}
				HandleDefault(child_idx, child_default.first, child_default.second, *entry->second.child_map,
				              *result_entry->second.child_remaps);
			}
			return;
		}
		// non-nested type - add it to the map
		if (default_type != target_type) {
			throw BinderException("Default key %s does not match target type %s - add a cast", default_target,
			                      target_type);
		}
		auto added = result.emplace(default_target, std::move(remap));
		if (!added.second) {
			throw BinderException("Duplicate value provided for target %s", default_target);
		}
	}

	static vector<RemapColumnInfo> ConstructMapFromChildren(const child_list_t<LogicalType> &target_children,
	                                                        const case_insensitive_map_t<RemapEntry> &remap_map) {
		vector<RemapColumnInfo> result;
		for (idx_t target_idx = 0; target_idx < target_children.size(); target_idx++) {
			auto &target_name = target_children[target_idx].first;
			auto &child_type = target_children[target_idx].second;
			auto entry = remap_map.find(target_name);
			if (entry == remap_map.end()) {
				throw BinderException("Missing target value %s for remap", target_name);
			}
			RemapColumnInfo info;
			info.index = entry->second.index;
			info.default_index = entry->second.default_index;
			if (IsRemappable(child_type) && entry->second.child_remaps) {
				// type is nested and a mapping for it is given - recurse
				info.child_remap_info = ConstructMap(child_type, *entry->second.child_remaps);
			}
			result.push_back(std::move(info));
		}
		return result;
	}

	static vector<RemapColumnInfo> ConstructMap(const LogicalType &type,
	                                            const case_insensitive_map_t<RemapEntry> &remap_map) {
		D_ASSERT(IsRemappable(type));
		switch (type.id()) {
		case LogicalTypeId::STRUCT: {
			auto &target_children = StructType::GetChildTypes(type);
			return ConstructMapFromChildren(target_children, remap_map);
		}
		case LogicalTypeId::LIST: {
			auto &child_type = ListType::GetChildType(type);
			child_list_t<LogicalType> target_children;
			target_children.emplace_back("list", child_type);
			return ConstructMapFromChildren(target_children, remap_map);
		}
		case LogicalTypeId::MAP: {
			auto &key_type = MapType::KeyType(type);
			auto &value_type = MapType::ValueType(type);
			child_list_t<LogicalType> target_children;
			target_children.emplace_back("key", key_type);
			target_children.emplace_back("value", value_type);
			return ConstructMapFromChildren(target_children, remap_map);
		}
		default:
			throw BinderException("Can't ConstructMap for type '%s'", type.ToString());
		}
	}

	static child_list_t<LogicalType> RemapCastChildren(const child_list_t<LogicalType> &source_children,
	                                                   const case_insensitive_map_t<RemapEntry> &remap_map,
	                                                   const unordered_map<idx_t, string> &source_name_map) {
		child_list_t<LogicalType> new_source_children;
		for (idx_t source_idx = 0; source_idx < source_children.size(); source_idx++) {
			auto &child_name = source_children[source_idx].first;
			auto &child_type = source_children[source_idx].second;
			auto entry = source_name_map.find(source_idx);
			if (entry != source_name_map.end()) {
				auto remap_entry = remap_map.find(entry->second);
				D_ASSERT(remap_entry != remap_map.end());
				// this entry is remapped - fetch the target type
				if (IsRemappable(child_type) && remap_entry->second.child_remaps) {
					// type is nested and a mapping for it is given - recurse
					new_source_children.emplace_back(child_name,
					                                 RemapCast(child_type, *remap_entry->second.child_remaps));
				} else {
					new_source_children.emplace_back(child_name, remap_entry->second.target_type);
				}
			} else {
				// entry is not remapped - keep the original type
				new_source_children.push_back(source_children[source_idx]);
			}
		}
		return new_source_children;
	}

	static LogicalType RemapCast(const LogicalType &type, const case_insensitive_map_t<RemapEntry> &remap_map) {
		unordered_map<idx_t, string> source_name_map;
		for (auto &entry : remap_map) {
			if (entry.second.index.IsValid()) {
				source_name_map.emplace(entry.second.index.GetIndex(), entry.first);
			}
		}

		switch (type.id()) {
		case LogicalTypeId::STRUCT: {
			auto &source_children = StructType::GetChildTypes(type);
			return LogicalType::STRUCT(RemapCastChildren(source_children, remap_map, source_name_map));
		}
		case LogicalTypeId::LIST: {
			auto &child_type = ListType::GetChildType(type);

			child_list_t<LogicalType> source_children;
			source_children.emplace_back("list", child_type);

			auto new_source_children = RemapCastChildren(source_children, remap_map, source_name_map);
			D_ASSERT(new_source_children.size() == 1);
			return LogicalType::LIST(new_source_children[0].second);
		}
		case LogicalTypeId::MAP: {
			auto &key_type = MapType::KeyType(type);
			auto &value_type = MapType::ValueType(type);

			child_list_t<LogicalType> source_children;
			source_children.emplace_back("key", key_type);
			source_children.emplace_back("value", value_type);

			auto new_source_children = RemapCastChildren(source_children, remap_map, source_name_map);
			D_ASSERT(new_source_children.size() == 2);
			return LogicalType::MAP(new_source_children[0].second, new_source_children[1].second);
		}
		default:
			throw BinderException("Can't RemapCast for type '%s'", type.ToString());
		}
	}
};

unique_ptr<FunctionData> RemapStructBind(ClientContext &context, ScalarFunction &bound_function,
                                         vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(arguments.size() == 4);
	for (idx_t arg_idx = 0; arg_idx < 3; arg_idx++) {
		auto &arg = arguments[arg_idx];
		if (arg->return_type.id() == LogicalTypeId::UNKNOWN) {
			throw ParameterNotResolvedException();
		}
		if (arg->return_type.id() == LogicalTypeId::SQLNULL && arg_idx == 2) {
			// remap target can be NULL
			continue;
		}
		if (!IsRemappable(arg->return_type)) {
			throw BinderException("Struct remap can only remap nested types, not '%s'", arg->return_type.ToString());
		} else if (arg->return_type.id() == LogicalTypeId::STRUCT && StructType::IsUnnamed(arg->return_type)) {
			throw BinderException("Struct remap can only remap named structs");
		}
	}
	auto &from_type = arguments[0]->return_type;
	auto &to_type = arguments[1]->return_type;

	auto &defaults = arguments[3];
	if (defaults->return_type.id() != LogicalTypeId::SQLNULL && defaults->return_type.id() != LogicalTypeId::STRUCT) {
		throw BinderException("The defaults provided to 'remap_struct' should be of type STRUCT if they're not NULL");
	}
	if (defaults->return_type.id() == LogicalTypeId::STRUCT && StructType::IsUnnamed(defaults->return_type)) {
		throw BinderException("The defaults have to be either NULL or a named STRUCT, not an unnamed struct");
	}

	if ((IsRemappable(from_type) || IsRemappable(to_type)) && from_type.id() != to_type.id()) {
		throw BinderException("Can't change source type (%s) to target type (%s), type conversion not allowed",
		                      from_type.ToString(), to_type.ToString());
	}

	if (!arguments[2]->IsFoldable()) {
		throw BinderException("Remap keys for remap_struct needs to be a constant value");
	}
	auto source_map = RemapIndex::GetMap(from_type);
	auto target_map = RemapIndex::GetMap(to_type);

	Value remap_val = ExpressionExecutor::EvaluateScalar(context, *arguments[2]);

	// (recursively) generate the remap entries
	case_insensitive_map_t<RemapEntry> remap_map;
	if (!remap_val.IsNull()) {
		auto &remap_types = StructType::GetChildTypes(arguments[2]->return_type);
		auto &remap_values = StructValue::GetChildren(remap_val);
		for (idx_t remap_idx = 0; remap_idx < remap_values.size(); remap_idx++) {
			auto &remap_val = remap_values[remap_idx];
			auto &remap_target = remap_types[remap_idx].first;
			RemapEntry::PerformRemap(remap_target, remap_val, source_map, target_map, remap_map, from_type);
		}
	}
	if (!arguments[3]->IsFoldable()) {
		throw BinderException("Default values must be constants");
	}

	if (arguments[3]->return_type.id() != LogicalTypeId::SQLNULL) {
		// (recursively) handle the defaults (if there are any)
		auto &default_types = StructType::GetChildTypes(arguments[3]->return_type);
		for (idx_t default_idx = 0; default_idx < default_types.size(); default_idx++) {
			auto &default_target = default_types[default_idx].first;
			auto &default_type = default_types[default_idx].second;
			RemapEntry::HandleDefault(default_idx, default_target, default_type, target_map, remap_map);
		}
	}

	// construct the final remapping
	auto remap = RemapEntry::ConstructMap(to_type, remap_map);

	// push a cast for argument 0 to match up the source types to the target
	auto new_type = RemapEntry::RemapCast(from_type, remap_map);

	bound_function.arguments[0] = std::move(new_type);
	bound_function.arguments[1] = arguments[1]->return_type;
	bound_function.arguments[2] = arguments[2]->return_type;
	bound_function.arguments[3] = arguments[3]->return_type;
	bound_function.SetReturnType(arguments[1]->return_type);

	return make_uniq<RemapStructBindData>(std::move(remap));
}

} // namespace

ScalarFunction RemapStructFun::GetFunction() {
	ScalarFunction remap("remap_struct",
	                     {LogicalTypeId::ANY, LogicalTypeId::ANY, LogicalTypeId::ANY, LogicalTypeId::ANY},
	                     LogicalTypeId::ANY, RemapStructFunction, RemapStructBind);
	remap.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return remap;
}

} // namespace duckdb
