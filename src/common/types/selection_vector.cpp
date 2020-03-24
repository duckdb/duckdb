#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/printer.hpp"

namespace duckdb {

string SelectionVector::ToString(idx_t count) {
	string result = "Selection Vector (" + to_string(count) + ") [";
	for(idx_t i = 0; i < count; i++) {
		if (i != 0) {
			result += ", ";
		}
		result += to_string(get_index(i));
	}
	result += "]";
	return result;
}

void SelectionVector::Print(idx_t count) {
	Printer::Print(ToString(count));
}

void SelectionVector::Slice(const SelectionVector &sel, idx_t count) {
	auto data = make_buffer<SelectionData>(count);
	auto result_ptr = data->owned_data.get();
	// for every element, we perform result[i] = target[new[i]]
	for(idx_t i = 0; i < count; i++) {
		auto new_idx = sel.get_index(i);
		auto idx = this->get_index(new_idx);
		result_ptr[i] = idx;
	}
	Initialize(move(data));
}

}
