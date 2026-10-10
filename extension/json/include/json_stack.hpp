#pragma once
#include "json_common.hpp"

namespace duckdb {
template <class STACK_ITEM> // preferably a struct
class Stack {
public:
	void Push(STACK_ITEM item) {
		stack.push_back(std::move(item));
	}
	STACK_ITEM Pop() {
		STACK_ITEM item = stack.back();
		stack.pop_back();
		return item;
	}
	bool Empty() const {
		return stack.empty();
	}

private:
	vector<STACK_ITEM> stack;
};

//! Iterative deep copy of a mutable value. yyjson_mut_val_mut_copy recurses into every nested array/object, so a deeply
//! nested document overflows the stack; this copies the containers level by level with an explicit stack instead.
static inline yyjson_mut_val *CopyMutableValIterative(yyjson_mut_doc *doc, yyjson_mut_val *val) {
	if (!doc || !val) {
		return nullptr;
	}
	// Create an empty shell of the same container kind, or a full copy for a (non-recursive) scalar/string value.
	auto shell = [&](yyjson_mut_val *src) -> yyjson_mut_val * {
		if (yyjson_mut_is_obj(src)) {
			return yyjson_mut_obj(doc);
		}
		if (yyjson_mut_is_arr(src)) {
			return yyjson_mut_arr(doc);
		}
		return yyjson_mut_val_mut_copy(doc, src);
	};

	auto result = shell(val);

	struct copy_item {
		yyjson_mut_val *src;
		yyjson_mut_val *dst;
	};
	Stack<copy_item> stack;
	stack.Push(copy_item {val, result});
	while (!stack.Empty()) {
		auto item = stack.Pop();
		if (yyjson_mut_is_arr(item.src)) {
			size_t idx, max;
			yyjson_mut_val *child;
			yyjson_mut_arr_foreach(item.src, idx, max, child) {
				auto child_copy = shell(child);
				yyjson_mut_arr_append(item.dst, child_copy);
				if (yyjson_mut_is_arr(child) || yyjson_mut_is_obj(child)) {
					stack.Push(copy_item {child, child_copy});
				}
			}
		} else if (yyjson_mut_is_obj(item.src)) {
			size_t idx, max;
			yyjson_mut_val *key, *child;
			yyjson_mut_obj_foreach(item.src, idx, max, key, child) {
				auto key_copy = yyjson_mut_val_mut_copy(doc, key);
				auto child_copy = shell(child);
				yyjson_mut_obj_add(item.dst, key_copy, child_copy);
				if (yyjson_mut_is_arr(child) || yyjson_mut_is_obj(child)) {
					stack.Push(copy_item {child, child_copy});
				}
			}
		}
	}
	return result;
}

} // namespace duckdb
