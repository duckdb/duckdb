#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

//! Coalescing deep merge: null in patch means "absent/unknown", keeps the original value.
//! Non-null patch values overwrite. Nested objects are merged recursively.
static yyjson_mut_val *DeepMerge(yyjson_mut_doc *doc, yyjson_mut_val *orig, yyjson_mut_val *patch) {
	// If patch is not an object, it replaces orig entirely (unless null)
	if (!yyjson_mut_is_obj(patch)) {
		// Null in patch means "no data" -- keep original if available
		if (unsafe_yyjson_is_null(patch) && orig) {
			return yyjson_mut_val_mut_copy(doc, orig);
		}
		return yyjson_mut_val_mut_copy(doc, patch);
	}

	auto builder = yyjson_mut_obj(doc);

	// If orig is not an object, treat it as an empty object
	yyjson_mut_val local_orig;
	memset(&local_orig, 0, sizeof(local_orig));
	if (!yyjson_mut_is_obj(orig)) {
		local_orig.tag = builder->tag;
		local_orig.uni = builder->uni;
		orig = &local_orig;
	}

	// Copy all keys from orig that are NOT in patch (or where patch value is null)
	if (orig != &local_orig) {
		idx_t idx, max;
		yyjson_mut_val *key, *orig_val;
		yyjson_mut_obj_foreach(orig, idx, max, key, orig_val) {
			auto patch_val = yyjson_mut_obj_getn(patch, unsafe_yyjson_get_str(key), unsafe_yyjson_get_len(key));
			if (!patch_val || unsafe_yyjson_is_null(patch_val)) {
				auto mut_key = yyjson_mut_val_mut_copy(doc, key);
				auto mut_val = yyjson_mut_val_mut_copy(doc, orig_val);
				yyjson_mut_obj_add(builder, mut_key, mut_val);
			}
		}
	}

	// Merge non-null items from patch -- null values are skipped (original preserved above)
	{
		idx_t idx, max;
		yyjson_mut_val *key, *patch_val;
		yyjson_mut_obj_foreach(patch, idx, max, key, patch_val) {
			if (unsafe_yyjson_is_null(patch_val)) {
				continue;
			}
			auto mut_key = yyjson_mut_val_mut_copy(doc, key);
			auto orig_val = yyjson_mut_obj_getn(orig, unsafe_yyjson_get_str(key), unsafe_yyjson_get_len(key));
			auto merged_val = DeepMerge(doc, orig_val, patch_val);
			yyjson_mut_obj_add(builder, mut_key, merged_val);
		}
	}

	return builder;
}

static inline void DeepMergeReadObjects(yyjson_mut_doc *doc, Vector &input, yyjson_mut_val *objs[], const idx_t count) {
	UnifiedVectorFormat input_data;
	auto &input_vector = input;
	input_vector.ToUnifiedFormat(count, input_data);
	auto inputs = UnifiedVectorFormat::GetData<string_t>(input_data);

	for (idx_t i = 0; i < count; i++) {
		auto idx = input_data.sel->get_index(i);
		if (!input_data.validity.RowIsValid(idx)) {
			objs[i] = nullptr;
		} else {
			objs[i] =
			    yyjson_val_mut_copy(doc, JSONCommon::ReadDocument(inputs[idx], JSONCommon::READ_FLAG, &doc->alc)->root);
		}
	}
}

static void DeepMergeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = JSONFunctionLocalState::ResetAndGet(state);
	auto alc = lstate.json_allocator->GetYYAlc();

	auto doc = JSONCommon::CreateDocument(alc);
	const auto count = args.size();

	// Read the first json arg
	auto origs = JSONCommon::AllocateArray<yyjson_mut_val *>(alc, count);
	DeepMergeReadObjects(doc, args.data[0], origs, count);

	// Read the next json args one by one and merge them into the first json arg
	auto patches = JSONCommon::AllocateArray<yyjson_mut_val *>(alc, count);
	for (idx_t arg_idx = 1; arg_idx < args.data.size(); arg_idx++) {
		DeepMergeReadObjects(doc, args.data[arg_idx], patches, count);
		for (idx_t i = 0; i < count; i++) {
			if (patches[i] == nullptr) {
				// Next json arg is NULL, obj becomes NULL
				origs[i] = nullptr;
			} else if (origs[i] == nullptr) {
				// Current obj is NULL, obj becomes next json arg
				origs[i] = patches[i];
			} else {
				// Neither is NULL, deep merge them
				origs[i] = DeepMerge(doc, origs[i], patches[i]);
			}
		}
	}

	// Write to result vector
	auto result_data = FlatVector::GetData<string_t>(result);
	auto &result_validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		if (origs[i] == nullptr) {
			result_validity.SetInvalid(i);
		} else {
			result_data[i] = JSONCommon::WriteVal<yyjson_mut_val>(origs[i], alc);
		}
	}

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	JSONAllocator::AddBuffer(result, alc);
}

ScalarFunctionSet JSONFunctions::GetDeepMergeFunction() {
	ScalarFunction fun("json_deep_merge", {LogicalType::JSON(), LogicalType::JSON()}, LogicalType::JSON(),
	                   DeepMergeFunction, nullptr, nullptr, nullptr, JSONFunctionLocalState::Init);
	fun.varargs = LogicalType::JSON();
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);

	return ScalarFunctionSet(fun);
}

} // namespace duckdb
