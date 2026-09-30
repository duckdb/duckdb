//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/function_set_base.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function.hpp"

namespace duckdb {

template <class T>
class FunctionSet {
public:
	explicit FunctionSet(Identifier name) : name(std::move(name)) { // NOLINT
	}

	//! The name of the function set
	Identifier name;

public:
	void SetName(Identifier name_p) {
		name = std::move(name_p);
	}
	//! The set of functions. The overloads are immutable and shared with every function bound from them - use
	//! ApplyToFunctions() to change them.
	vector<shared_ptr<const T>> functions;

public:
	void AddFunction(T function) {
		functions.push_back(make_shared_ptr<T>(std::move(function)));
	}
	void AddFunction(shared_ptr<const T> function) {
		functions.push_back(std::move(function));
	}
	idx_t Size() {
		return functions.size();
	}

	const shared_ptr<const T> &GetFunctionByOffset(idx_t offset) const {
		D_ASSERT(offset < functions.size());
		return functions[offset];
	}

	//! Apply a modification to every overload in the set. The overloads are immutable once bound from, so each is
	//! replaced by a modified copy rather than being changed in place.
	template <class FUNC>
	void ApplyToFunctions(FUNC &&callback) {
		for (auto &function : functions) {
			auto modified = make_shared_ptr<T>(*function);
			callback(*modified);
			function = std::move(modified);
		}
	}

	bool MergeFunctionSet(FunctionSet<T> new_functions, bool override = false) {
		D_ASSERT(!new_functions.functions.empty());
		for (auto &new_func : new_functions.functions) {
			bool overwritten = false;
			for (auto &func : functions) {
				if (new_func->Equal(*func)) {
					// function overload already exists
					if (override) {
						// override it
						overwritten = true;
						func = new_func;
					} else {
						// throw an error
						return false;
					}
					break;
				}
			}
			if (!overwritten) {
				functions.push_back(new_func);
			}
		}
		return true;
	}
};

} // namespace duckdb
