#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

//! Recursively remove all object keys with null values
static void StripNulls(yyjson_mut_val *val) {
	if (!val) {
		return;
	}
	if (yyjson_mut_is_obj(val)) {
		yyjson_mut_obj_iter iter;
		yyjson_mut_obj_iter_init(val, &iter);
		yyjson_mut_val *key;
		while ((key = yyjson_mut_obj_iter_next(&iter)) != nullptr) {
			auto child = yyjson_mut_obj_iter_get_val(key);
			if (unsafe_yyjson_is_null(child)) {
				yyjson_mut_obj_iter_remove(&iter);
			} else {
				StripNulls(child);
			}
		}
	} else if (yyjson_mut_is_arr(val)) {
		idx_t idx, max;
		yyjson_mut_val *elem;
		yyjson_mut_arr_foreach(val, idx, max, elem) {
			StripNulls(elem);
		}
	}
}

//! Strip all null-valued keys from a JSON document recursively
static void StripNullsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = JSONFunctionLocalState::ResetAndGet(state);
	auto alc = lstate.json_allocator->GetYYAlc();

	auto &inputs = args.data[0];
	UnaryExecutor::ExecuteWithNulls<string_t, string_t>(
	    inputs, result, args.size(), [&](string_t input, ValidityMask &mask, idx_t idx) {
		    auto doc = JSONCommon::ReadDocument(input, JSONCommon::READ_FLAG, alc);
		    auto mut_doc = yyjson_doc_mut_copy(doc, alc);
		    auto root = yyjson_mut_doc_get_root(mut_doc);
		    StripNulls(root);
		    return JSONCommon::WriteVal<yyjson_mut_val>(root, alc);
	    });

	JSONAllocator::AddBuffer(result, alc);
}

static void GetStripNullsFunctionInternal(ScalarFunctionSet &set, const LogicalType &json) {
	set.AddFunction(ScalarFunction("json_strip_nulls", {json}, LogicalType::JSON(), StripNullsFunction, nullptr,
	                               nullptr, JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetStripNullsFunction() {
	ScalarFunctionSet set("json_strip_nulls");
	GetStripNullsFunctionInternal(set, LogicalType::JSON());
	return set;
}

} // namespace duckdb
