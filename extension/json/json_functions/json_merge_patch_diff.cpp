#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

//! Internal recursive diff. Returns nullptr to signal "no changes" to the caller,
//! which is used to skip unchanged keys in the parent object's diff.
static yyjson_mut_val *ComputeDiff(yyjson_mut_doc *doc, yyjson_val *old_val, yyjson_val *new_val) {
	// Both objects: compute recursive structural diff
	if (yyjson_is_obj(old_val) && yyjson_is_obj(new_val)) {
		auto builder = yyjson_mut_obj(doc);
		bool has_diff = false;

		// Keys in old but not in new: removed (emit null)
		{
			idx_t idx, max;
			yyjson_val *key, *old_child;
			yyjson_obj_foreach(old_val, idx, max, key, old_child) {
				if (!yyjson_obj_getn(new_val, unsafe_yyjson_get_str(key), unsafe_yyjson_get_len(key))) {
					yyjson_mut_obj_add(builder, yyjson_val_mut_copy(doc, key), yyjson_mut_null(doc));
					has_diff = true;
				}
			}
		}

		// Keys in new: added or changed
		{
			idx_t idx, max;
			yyjson_val *key, *new_child;
			yyjson_obj_foreach(new_val, idx, max, key, new_child) {
				auto old_child = yyjson_obj_getn(old_val, unsafe_yyjson_get_str(key), unsafe_yyjson_get_len(key));
				if (!old_child) {
					// Key added
					yyjson_mut_obj_add(builder, yyjson_val_mut_copy(doc, key), yyjson_val_mut_copy(doc, new_child));
					has_diff = true;
				} else {
					// Key exists in both: recurse
					auto sub_diff = ComputeDiff(doc, old_child, new_child);
					if (sub_diff) {
						yyjson_mut_obj_add(builder, yyjson_val_mut_copy(doc, key), sub_diff);
						has_diff = true;
					}
				}
			}
		}

		return has_diff ? builder : nullptr;
	}

	// Not both objects: use yyjson's built-in deep equality
	if (yyjson_equals(old_val, new_val)) {
		return nullptr;
	}
	return yyjson_val_mut_copy(doc, new_val);
}

//! Compute the minimal RFC 7396 merge patch that transforms old_val into new_val.
//! Both objects: returns the structural diff (or empty object {} if equal).
//! Otherwise: returns a copy of new_val.
static yyjson_mut_val *MergePatchDiff(yyjson_mut_doc *doc, yyjson_val *old_val, yyjson_val *new_val) {
	if (yyjson_is_obj(old_val) && yyjson_is_obj(new_val)) {
		auto diff = ComputeDiff(doc, old_val, new_val);
		return diff ? diff : yyjson_mut_obj(doc);
	}
	return yyjson_val_mut_copy(doc, new_val);
}

//! Compute the RFC 7396 merge patch that transforms old into new
static void MergePatchDiffFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = JSONFunctionLocalState::ResetAndGet(state);
	auto alc = lstate.json_allocator->GetYYAlc();

	auto doc = JSONCommon::CreateDocument(alc);
	const auto count = args.size();

	UnifiedVectorFormat old_data, new_data;
	args.data[0].ToUnifiedFormat(count, old_data);
	args.data[1].ToUnifiedFormat(count, new_data);
	auto old_inputs = UnifiedVectorFormat::GetData<string_t>(old_data);
	auto new_inputs = UnifiedVectorFormat::GetData<string_t>(new_data);

	auto result_data = FlatVector::Writer<string_t>(result, count);

	for (idx_t i = 0; i < count; i++) {
		auto old_idx = old_data.sel->get_index(i);
		auto new_idx = new_data.sel->get_index(i);

		if (!new_data.validity.RowIsValid(new_idx)) {
			result_data.WriteNull();
			continue;
		}

		auto new_doc = JSONCommon::ReadDocument(new_inputs[new_idx], JSONCommon::READ_FLAG, alc);

		if (!old_data.validity.RowIsValid(old_idx)) {
			result_data.WriteStringRef(JSONCommon::WriteVal<yyjson_val>(new_doc->root, alc));
			continue;
		}

		auto old_doc = JSONCommon::ReadDocument(old_inputs[old_idx], JSONCommon::READ_FLAG, alc);
		auto diff = MergePatchDiff(doc, old_doc->root, new_doc->root);
		result_data.WriteStringRef(JSONCommon::WriteVal<yyjson_mut_val>(diff, alc));
	}
	JSONAllocator::AddBuffer(result, alc);
}

ScalarFunctionSet JSONFunctions::GetMergePatchDiffFunction() {
	ScalarFunction fun("json_merge_patch_diff", {LogicalType::JSON(), LogicalType::JSON()}, LogicalType::JSON(),
	                   MergePatchDiffFunction, nullptr, nullptr, JSONFunctionLocalState::Init);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);

	return ScalarFunctionSet(fun);
}

} // namespace duckdb
