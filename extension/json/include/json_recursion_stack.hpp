#pragma once
#include "json_common.hpp"

namespace duckdb {
template<class STACK_ITEM> // preferably a struct
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

template<class RETURN_TYPE, class STACK_ITEM>
static RETURN_TYPE JSONIterator(std::function base_case, std::function iter_case) {
	// lets have some void function, we run the loop and apply that function until the base case

	// two functio types -- unary (i.e. edit in place, transform the input into the output
	// binary -- take two inputs and output something that
}
}