#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

//! Recursively sort all object keys alphabetically (byte-order) in a mutable JSON value
static void SortKeys(yyjson_mut_val *val) {
	if (!val) {
		return;
	}
	if (yyjson_mut_is_obj(val)) {
		idx_t size = yyjson_mut_obj_size(val);
		if (size <= 1) {
			if (size == 1) {
				yyjson_mut_obj_iter iter;
				yyjson_mut_obj_iter_init(val, &iter);
				yyjson_mut_val *key = yyjson_mut_obj_iter_next(&iter);
				SortKeys(yyjson_mut_obj_iter_get_val(key));
			}
			return;
		}

		// Extract key-value pairs
		vector<std::pair<yyjson_mut_val *, yyjson_mut_val *>> pairs;
		pairs.reserve(size);
		yyjson_mut_obj_iter iter;
		yyjson_mut_obj_iter_init(val, &iter);
		yyjson_mut_val *key;
		while ((key = yyjson_mut_obj_iter_next(&iter)) != nullptr) {
			pairs.emplace_back(key, yyjson_mut_obj_iter_get_val(key));
		}

		// Sort by key string (byte-order comparison)
		std::sort(pairs.begin(), pairs.end(),
		          [](const std::pair<yyjson_mut_val *, yyjson_mut_val *> &a,
		             const std::pair<yyjson_mut_val *, yyjson_mut_val *> &b) {
			          const char *a_str = unsafe_yyjson_get_str(a.first);
			          idx_t a_len = unsafe_yyjson_get_len(a.first);
			          const char *b_str = unsafe_yyjson_get_str(b.first);
			          idx_t b_len = unsafe_yyjson_get_len(b.first);
			          int cmp = memcmp(a_str, b_str, MinValue(a_len, b_len));
			          if (cmp != 0) {
				          return cmp < 0;
			          }
			          return a_len < b_len;
		          });

		// Recursively sort nested values
		for (auto &pair : pairs) {
			SortKeys(pair.second);
		}

		// Clear and rebuild in sorted order
		yyjson_mut_obj_clear(val);
		for (auto &pair : pairs) {
			yyjson_mut_obj_add(val, pair.first, pair.second);
		}
	} else if (yyjson_mut_is_arr(val)) {
		yyjson_mut_val *elem;
		yyjson_mut_arr_iter iter;
		yyjson_mut_arr_iter_init(val, &iter);
		while ((elem = yyjson_mut_arr_iter_next(&iter)) != nullptr) {
			SortKeys(elem);
		}
	}
}

//! Serialize JSON with all object keys sorted, enabling deterministic hashing
static void SerializeSortedFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = JSONFunctionLocalState::ResetAndGet(state);
	auto alc = lstate.json_allocator->GetYYAlc();

	const auto count = args.size();
	UnifiedVectorFormat input_data;
	args.data[0].ToUnifiedFormat(count, input_data);
	auto inputs = UnifiedVectorFormat::GetData<string_t>(input_data);

	auto result_data = FlatVector::GetData<string_t>(result);
	auto &result_validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		auto idx = input_data.sel->get_index(i);
		if (!input_data.validity.RowIsValid(idx)) {
			result_validity.SetInvalid(i);
			continue;
		}

		auto doc = JSONCommon::ReadDocument(inputs[idx], JSONCommon::READ_FLAG, alc);
		auto mut_doc = yyjson_doc_mut_copy(doc, alc);
		auto root = yyjson_mut_doc_get_root(mut_doc);

		SortKeys(root);

		result_data[i] = JSONCommon::WriteVal<yyjson_mut_val>(root, alc);
	}

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	JSONAllocator::AddBuffer(result, alc);
}

static void GetSerializeSortedFunctionInternal(ScalarFunctionSet &set, const LogicalType &json) {
	set.AddFunction(ScalarFunction("json_serialize_sorted", {json}, LogicalType::VARCHAR, SerializeSortedFunction,
	                               nullptr, nullptr, nullptr, JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetSerializeSortedFunction() {
	ScalarFunctionSet set("json_serialize_sorted");
	GetSerializeSortedFunctionInternal(set, LogicalType::JSON());
	return set;
}

} // namespace duckdb
