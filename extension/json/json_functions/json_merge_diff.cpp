#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

//! Fast equality check for non-object JSON values.
//! Avoids serialization for null, bool, and string (the common cases).
//! Falls back to serialization only for numbers and arrays.
static bool LeafValuesEqual(yyjson_val *old_val, yyjson_val *new_val, yyjson_alc *alc) {
	// Different type or subtype: always different
	if (yyjson_get_tag(old_val) != yyjson_get_tag(new_val)) {
		return false;
	}
	// Null or bool: tag match is sufficient (subtype encodes true/false)
	if (unsafe_yyjson_is_null(old_val) || yyjson_is_bool(old_val)) {
		return true;
	}
	// String: compare length then content directly (no serialization)
	if (yyjson_is_str(old_val)) {
		auto old_len = unsafe_yyjson_get_len(old_val);
		if (old_len != unsafe_yyjson_get_len(new_val)) {
			return false;
		}
		return memcmp(unsafe_yyjson_get_str(old_val), unsafe_yyjson_get_str(new_val), old_len) == 0;
	}
	// Numbers, arrays: serialize and compare
	auto old_str = JSONCommon::WriteVal<yyjson_val>(old_val, alc);
	auto new_str = JSONCommon::WriteVal<yyjson_val>(new_val, alc);
	return old_str == new_str;
}

//! Compute the minimal RFC 7396 merge patch that transforms old_val into new_val.
//! Inputs are immutable yyjson_val; only the diff result is built as mutable.
//! Returns nullptr when old_val and new_val are equal (no diff).
static yyjson_mut_val *MergeDiff(yyjson_mut_doc *doc, yyjson_val *old_val, yyjson_val *new_val, yyjson_alc *alc) {
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
					auto sub_diff = MergeDiff(doc, old_child, new_child, alc);
					if (sub_diff) {
						yyjson_mut_obj_add(builder, yyjson_val_mut_copy(doc, key), sub_diff);
						has_diff = true;
					}
				}
			}
		}

		return has_diff ? builder : nullptr;
	}

	// Not both objects: use fast leaf comparison
	if (LeafValuesEqual(old_val, new_val, alc)) {
		return nullptr;
	}
	return yyjson_val_mut_copy(doc, new_val);
}

//! Compute the RFC 7396 merge patch that transforms old into new
static void MergeDiffFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = JSONFunctionLocalState::ResetAndGet(state);
	auto alc = lstate.json_allocator->GetYYAlc();

	auto doc = JSONCommon::CreateDocument(alc);
	const auto count = args.size();

	UnifiedVectorFormat old_data, new_data;
	args.data[0].ToUnifiedFormat(count, old_data);
	args.data[1].ToUnifiedFormat(count, new_data);
	auto old_inputs = UnifiedVectorFormat::GetData<string_t>(old_data);
	auto new_inputs = UnifiedVectorFormat::GetData<string_t>(new_data);

	auto result_data = FlatVector::GetData<string_t>(result);
	auto &result_validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		auto old_idx = old_data.sel->get_index(i);
		auto new_idx = new_data.sel->get_index(i);

		if (!new_data.validity.RowIsValid(new_idx)) {
			result_validity.SetInvalid(i);
			continue;
		}

		auto new_doc = JSONCommon::ReadDocument(new_inputs[new_idx], JSONCommon::READ_FLAG, alc);

		if (!old_data.validity.RowIsValid(old_idx)) {
			result_data[i] = JSONCommon::WriteVal<yyjson_val>(new_doc->root, alc);
			continue;
		}

		auto old_doc = JSONCommon::ReadDocument(old_inputs[old_idx], JSONCommon::READ_FLAG, alc);

		if (yyjson_is_obj(old_doc->root) && yyjson_is_obj(new_doc->root)) {
			auto diff = MergeDiff(doc, old_doc->root, new_doc->root, alc);
			if (!diff) {
				diff = yyjson_mut_obj(doc);
			}
			result_data[i] = JSONCommon::WriteVal<yyjson_mut_val>(diff, alc);
		} else {
			result_data[i] = JSONCommon::WriteVal<yyjson_val>(new_doc->root, alc);
		}
	}

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	JSONAllocator::AddBuffer(result, alc);
}

ScalarFunctionSet JSONFunctions::GetMergeDiffFunction() {
	ScalarFunction fun("json_merge_diff", {LogicalType::JSON(), LogicalType::JSON()}, LogicalType::JSON(),
	                   MergeDiffFunction, nullptr, nullptr, nullptr, JSONFunctionLocalState::Init);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);

	return ScalarFunctionSet(fun);
}

} // namespace duckdb
