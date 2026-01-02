#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "core_functions/scalar/map_functions.hpp"

namespace duckdb {

namespace {

struct MapKeyIndexPair {
	MapKeyIndexPair(idx_t map, idx_t key) : map_index(map), key_index(key) {
	}
	// The index of the map that this key comes from
	idx_t map_index;
	// The index within the maps key_list
	idx_t key_index;
};

vector<Value> GetListEntries(vector<Value> keys, vector<Value> values) {
	D_ASSERT(keys.size() == values.size());
	vector<Value> entries;
	for (idx_t i = 0; i < keys.size(); i++) {
		child_list_t<Value> children;
		children.emplace_back(make_pair("key", std::move(keys[i])));
		children.emplace_back(make_pair("value", std::move(values[i])));
		entries.push_back(Value::STRUCT(std::move(children)));
	}
	return entries;
}

void MapConcatFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	if (result.GetType().id() == LogicalTypeId::SQLNULL) {
		// All inputs are NULL, just return NULL
		auto &validity = FlatVector::Validity(result);
		validity.SetInvalid(0);
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		return;
	}
	D_ASSERT(result.GetType().id() == LogicalTypeId::MAP);
	auto count = args.size();

	auto map_count = args.ColumnCount();
	vector<UnifiedVectorFormat> map_formats(map_count);
	for (idx_t i = 0; i < map_count; i++) {
		auto &map = args.data[i];
		map.ToUnifiedFormat(count, map_formats[i]);
	}
	auto result_data = FlatVector::GetData<list_entry_t>(result);

	for (idx_t i = 0; i < count; i++) {
		// Loop through all the maps per list
		// we cant do better because all the entries of the child vector have to be contiguous
		// so we cant start the next row before we have finished the one before it
		auto &result_entry = result_data[i];
		vector<MapKeyIndexPair> index_to_map;
		vector<Value> keys_list;
		bool all_null = true;
		for (idx_t map_idx = 0; map_idx < map_count; map_idx++) {
			if (args.data[map_idx].GetType().id() == LogicalTypeId::SQLNULL) {
				continue;
			}

			auto &map_format = map_formats[map_idx];
			auto index = map_format.sel->get_index(i);
			if (!map_format.validity.RowIsValid(index)) {
				continue;
			}

			all_null = false;
			auto &keys = MapVector::GetKeys(args.data[map_idx]);
			auto entry = UnifiedVectorFormat::GetData<list_entry_t>(map_format)[index];

			// Update the list for this row
			for (idx_t list_idx = 0; list_idx < entry.length; list_idx++) {
				auto key_index = entry.offset + list_idx;
				auto key = keys.GetValue(key_index);
				auto entry = std::find(keys_list.begin(), keys_list.end(), key);
				if (entry == keys_list.end()) {
					// Result list does not contain this value yet
					keys_list.push_back(key);
					index_to_map.emplace_back(map_idx, key_index);
				} else {
					// Result list already contains this, update where to find the value at
					auto distance = std::distance(keys_list.begin(), entry);
					auto &mapping = *(index_to_map.begin() + distance);
					mapping.key_index = key_index;
					mapping.map_index = map_idx;
				}
			}
		}

		result_entry.offset = ListVector::GetListSize(result);
		result_entry.length = keys_list.size();
		if (all_null) {
			D_ASSERT(keys_list.empty() && index_to_map.empty());
			FlatVector::SetNull(result, i, true);
			continue;
		}

		vector<Value> values_list;
		D_ASSERT(keys_list.size() == index_to_map.size());
		// Get the values from the mapping
		for (auto &mapping : index_to_map) {
			auto &map = args.data[mapping.map_index];
			auto &values = MapVector::GetValues(map);
			values_list.push_back(values.GetValue(mapping.key_index));
		}
		D_ASSERT(values_list.size() == keys_list.size());
		auto list_entries = GetListEntries(std::move(keys_list), std::move(values_list));
		for (auto &list_entry : list_entries) {
			ListVector::PushBack(result, list_entry);
		}
	}

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	result.Verify(count);
}

bool IsEmptyMap(const LogicalType &map) {
	D_ASSERT(map.id() == LogicalTypeId::MAP);
	auto &key_type = MapType::KeyType(map);
	auto &value_type = MapType::ValueType(map);
	return key_type.id() == LogicalType::SQLNULL && value_type.id() == LogicalType::SQLNULL;
}

unique_ptr<FunctionData> MapConcatBind(ClientContext &context, ScalarFunction &bound_function,
                                       vector<unique_ptr<Expression>> &arguments) {
	auto arg_count = arguments.size();
	if (arg_count < 2) {
		throw InvalidInputException("The provided amount of arguments is incorrect, please provide 2 or more maps");
	}

	if (arguments[0]->return_type.id() == LogicalTypeId::UNKNOWN) {
		// Prepared statement
		bound_function.arguments.emplace_back(LogicalTypeId::UNKNOWN);
		bound_function.SetReturnType(LogicalTypeId::SQLNULL);
		return nullptr;
	}

	LogicalType expected = LogicalType::SQLNULL;

	bool is_null = true;
	// Check and verify that all the maps are of the same type
	for (idx_t i = 0; i < arg_count; i++) {
		auto &arg = arguments[i];
		auto &map = arg->return_type;
		if (map.id() == LogicalTypeId::UNKNOWN) {
			// Prepared statement
			bound_function.arguments.emplace_back(LogicalTypeId::UNKNOWN);
			bound_function.SetReturnType(LogicalTypeId::SQLNULL);
			return nullptr;
		}
		if (map.id() == LogicalTypeId::SQLNULL) {
			// The maps are allowed to be NULL
			continue;
		}
		if (map.id() != LogicalTypeId::MAP) {
			throw InvalidInputException("MAP_CONCAT only takes map arguments");
		}
		is_null = false;
		if (IsEmptyMap(map)) {
			// Map is allowed to be empty
			continue;
		}

		if (expected.id() == LogicalTypeId::SQLNULL) {
			expected = map;
		} else if (map != expected) {
			throw InvalidInputException(
			    "'value' type of map differs between arguments, expected '%s', found '%s' instead", expected.ToString(),
			    map.ToString());
		}
	}

	if (expected.id() == LogicalTypeId::SQLNULL && is_null == false) {
		expected = LogicalType::MAP(LogicalType::SQLNULL, LogicalType::SQLNULL);
	}
	bound_function.SetReturnType(expected);
	return make_uniq<VariableReturnBindData>(bound_function.GetReturnType());
}

} // namespace

ScalarFunction MapConcatFun::GetFunction() {
	//! the arguments and return types are actually set in the binder function
	ScalarFunction fun("map_concat", {}, LogicalTypeId::LIST, MapConcatFunction, MapConcatBind);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	fun.varargs = LogicalType::ANY;
	return fun;
}

} // namespace duckdb
