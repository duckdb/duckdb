#include "duckdb/function/table/arrow.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/types/arrow_aux_data.hpp"

namespace duckdb {

ArrowArrayScanState::ArrowArrayScanState(ArrowScanLocalState &state) : state(state) {
}

ArrowArrayScanState &ArrowArrayScanState::GetChild(idx_t child_idx) {
	auto it = children.find(child_idx);
	if (it == children.end()) {
		auto child_p = make_uniq<ArrowArrayScanState>(state);
		auto &child = *child_p;
		children.emplace(std::make_pair(child_idx, std::move(child_p)));
		return child;
	}
	if (!it->second->owned_data) {
		// Propagate down the ownership, for dictionaries in children
		it->second->owned_data = owned_data;
	}
	return *it->second;
}

void ArrowArrayScanState::AddDictionary(unique_ptr<Vector> dictionary_p) {
	dictionary = std::move(dictionary_p);
	D_ASSERT(owned_data);
	// Make sure the data referenced by the dictionary stays alive
	dictionary->GetBuffer()->SetAuxiliaryData(make_uniq<ArrowAuxiliaryData>(owned_data));
}

bool ArrowArrayScanState::HasDictionary() const {
	return dictionary != nullptr;
}

Vector &ArrowArrayScanState::GetDictionary() {
	D_ASSERT(HasDictionary());
	return *dictionary;
}

} // namespace duckdb
