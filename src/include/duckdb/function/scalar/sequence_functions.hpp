//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/sequence_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/built_in_functions.hpp"

namespace duckdb {

struct NextvalBindData : public FunctionData {
	explicit NextvalBindData(optional_ptr<SequenceCatalogEntry> sequence)
	    : sequence(sequence), create_info(sequence ? sequence->GetInfo() : nullptr) {
	}

	//! The sequence to use for the nextval computation; only if the sequence is a constant
	optional_ptr<SequenceCatalogEntry> sequence;

	//! The CreateInfo for the above sequence, if it exists
	unique_ptr<CreateInfo> create_info;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<NextvalBindData>(sequence);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<NextvalBindData>();
		return sequence == other.sequence;
	}
};

struct NextvalFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct CurrvalFun {
	static void RegisterFunction(BuiltinFunctions &set);
};
} // namespace duckdb
