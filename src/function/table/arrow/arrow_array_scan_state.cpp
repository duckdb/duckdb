#include "duckdb/function/table/arrow.hpp"

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
	return *it->second;
}

void ArrowArrayScanState::AddDictionary(unique_ptr<Vector> dictionary_p) {
	this->dictionary = std::move(dictionary_p);
}

bool ArrowArrayScanState::HasDictionary() const {
	return dictionary != nullptr;
}

Vector &ArrowArrayScanState::GetDictionary() {
	D_ASSERT(HasDictionary());
	return *dictionary;
}

} // namespace duckdb
