#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

//! Internal recursive diff. Returns nullptr to signal "no changes" to the caller,
//! which is used to skip unchanged keys in the parent object's diff.
// static yyjson_mut_val *ComputeDiff(yyjson_mut_doc *doc, yyjson_val *old_val, yyjson_val *new_val, idx_t depth = 0) {
// 	if (depth >= JSONCommon::MAX_RECURSION_DEPTH) {
// 		throw InvalidInputException("json_merge_patch_diff: JSON exceeds maximum recursion depth of %d",
// 		                            JSONCommon::MAX_RECURSION_DEPTH);
// 	}

// 	// Both objects: compute recursive structural diff
// 	if (yyjson_is_obj(old_val) && yyjson_is_obj(new_val)) {
// 		auto builder = yyjson_mut_obj(doc);
// 		bool has_diff = false;

// 		// Keys in old but not in new: removed (emit null)
// 		{
// 			idx_t idx, max;
// 			yyjson_val *key, *old_child;
// 			yyjson_obj_foreach(old_val, idx, max, key, old_child) {
// 				if (!yyjson_obj_getn(new_val, unsafe_yyjson_get_str(key), unsafe_yyjson_get_len(key))) {
// 					yyjson_mut_obj_add(builder, yyjson_val_mut_copy(doc, key), yyjson_mut_null(doc));
// 					has_diff = true;
// 				}
// 			}
// 		}

// 		// Keys in new: added or changed
// 		{
// 			idx_t idx, max;
// 			yyjson_val *key, *new_child;
// 			yyjson_obj_foreach(new_val, idx, max, key, new_child) {
// 				auto old_child = yyjson_obj_getn(old_val, unsafe_yyjson_get_str(key), unsafe_yyjson_get_len(key));
// 				if (!old_child) {
// 					// Key added
// 					yyjson_mut_obj_add(builder, yyjson_val_mut_copy(doc, key), yyjson_val_mut_copy(doc, new_child));
// 					has_diff = true;
// 				} else {
// 					// Key exists in both: recurse
// 					auto sub_diff = ComputeDiff(doc, old_child, new_child, depth + 1);
// 					if (sub_diff) {
// 						yyjson_mut_obj_add(builder, yyjson_val_mut_copy(doc, key), sub_diff);
// 						has_diff = true;
// 					}
// 				}
// 			}
// 		}

// 		return has_diff ? builder : nullptr;
// 	}

// 	// Not both objects: use yyjson's built-in deep equality
// 	if (yyjson_equals(old_val, new_val)) {
// 		return nullptr;
// 	}
// 	return yyjson_val_mut_copy(doc, new_val);
// }

static yyjson_mut_val *ComputeDiff(yyjson_mut_doc *doc, yyjson_val *old_val, yyjson_val *new_val) {
	fprintf(stderr, "\nfunc start\n");

	auto root_builder = yyjson_mut_obj(doc);

	struct stack_item {
		yyjson_mut_val *key;
		yyjson_val *old_node;
		yyjson_val *new_node;
		yyjson_mut_val *builder;
	};
	auto stack = std::vector<stack_item>();
	stack.emplace_back(stack_item {nullptr, old_val, new_val, root_builder});

	while (!stack.empty()) {
		fprintf(stderr, "loop start\n");
		auto nodes = stack.back();
		stack.pop_back();

		auto builder = nodes.builder;

		// Both objects: compute structural diff
		if (unsafe_yyjson_is_obj(nodes.old_node) && unsafe_yyjson_is_obj(nodes.new_node)) {
			// Keys in old but not in new: removed (emit null)
			{
				idx_t idx, max;
				yyjson_val *key, *old_child;
				yyjson_obj_foreach(nodes.old_node, idx, max, key, old_child) {
					if (!yyjson_obj_getn(nodes.new_node, unsafe_yyjson_get_str(key), unsafe_yyjson_get_len(key))) {
						fprintf(stderr, "key in old, but not in new\n");
						yyjson_mut_obj_add(builder, yyjson_val_mut_copy(doc, key), yyjson_mut_null(doc));
					}
				}
			}

			// Keys in new: added or changed
			{
				idx_t idx, max;
				yyjson_val *key, *new_child;
				yyjson_obj_foreach(nodes.new_node, idx, max, key, new_child) {
					auto old_child =
					    yyjson_obj_getn(nodes.old_node, unsafe_yyjson_get_str(key), unsafe_yyjson_get_len(key));
					auto mut_key = yyjson_val_mut_copy(doc, key);
					if (!old_child) {
						fprintf(stderr, "key was added to new, not in old %o\n", *key);
						// Key added
						yyjson_mut_obj_add(builder, mut_key, yyjson_val_mut_copy(doc, new_child));
					} else {
						// Key exists in both, compute diff for child
						if (unsafe_yyjson_is_obj(old_child) && unsafe_yyjson_is_obj(new_child)) {
							fprintf(stderr, "key is in both, check if they're different %o\n", *key);
							auto child_builder = yyjson_mut_obj(doc);
							auto *new_child_copy = new_child;
							stack.emplace_back(stack_item {mut_key, old_child, new_child_copy, child_builder});

							yyjson_mut_obj_add(builder, mut_key, child_builder);
						} else if (unsafe_yyjson_is_obj(old_child) && unsafe_yyjson_is_obj(new_child)) {
							// Key exists in both, both objects: defer to the stack (recurse)
							auto child_builder = yyjson_mut_obj(doc);
							stack.emplace_back(stack_item {mut_key, old_child, new_child, child_builder});
							yyjson_mut_obj_add(builder, mut_key, child_builder);
						} else if (!yyjson_equals(old_child, new_child)) {
							// Key exists in both, not both objects, and they differ: emit new value
							yyjson_mut_obj_add(builder, mut_key, yyjson_val_mut_copy(doc, new_child));
						}
					}
				}
			}
		} else if (!yyjson_equals(nodes.old_node,
		                          nodes.new_node)) { // either both are non-object, or one obj one non-obj
			fprintf(stderr, "old and new are NOT the same, add %o\n", nodes.new_node);
			yyjson_mut_obj_add(builder, nodes.key, yyjson_val_mut_copy(doc, nodes.new_node));
		} else if (yyjson_equals(nodes.old_node, nodes.new_node)) {
			yyjson_mut_obj_remove(builder, nodes.key);
		}
	}

	return yyjson_mut_obj_size(root_builder) == 0 ? nullptr : root_builder;
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
	args.data[0].ToUnifiedFormat(old_data);
	args.data[1].ToUnifiedFormat(new_data);
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
	fun.SetFallible();

	return ScalarFunctionSet(fun);
}

} // namespace duckdb
